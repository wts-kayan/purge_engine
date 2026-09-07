package com.bnp.str.purge.mapping

import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.time.LocalDate

object PrimaryView {

  private val log = LoggerFactory.getLogger(this.getClass)

  /** UDF names registered by [[PrimaryMapper]] and used by the query below. */
  val UDF_BUSINESS_DATE = "purge_business_date"
  val UDF_RETENTION_CUTOFF = "purge_retention_cutoff"

  /**
   * Load a query from the SQL queries file named by `purge_app.sql_queries.path`, the same escape
   * hatch the other modules provide: a perimeter with a selection rule the built-in query cannot
   * express sets `queryName` instead of forking the engine.
   */
  def loadQuery(queryName: String)(implicit sparkSession: SparkSession, config: Config): String = {
    val sqlQueriesPath = config.getString(s"${PrimaryConstants.APP_CONF}.sql_queries.path")
    val configPath = PrimaryUtilities.getHdfsReader(sqlQueriesPath)(sparkSession.sparkContext)
    val sqlQueriesConfig = ConfigFactory.parseReader(configPath)
    val sql = sqlQueriesConfig.getString(s"${PrimaryConstants.APP_CONF}.$queryName").stripMargin
    log.info(s"$queryName = $sql")
    sql
  }

  /**
   * The candidate-selection query: everything in the inventory that a retention policy has aged
   * out, plus everything a person explicitly selected in TWIST.
   *
   * It is deliberately ONE query rather than a chain of DataFrame steps, because what it decides —
   * which objects are eligible for deletion — is the part of the engine the business will want to
   * read, argue with, and eventually audit. `EXPLAIN` on a query is evidence; a chain of Scala
   * transformations is not.
   *
   * Reading it in order:
   *
   *  1. `registered_partition` / `registered_table` — what the metastore knows.
   *  2. `with_partition` — a path that IS a registered partition location gets its spec attached.
   *     Exact equality: a partition is one directory, and anything looser would attach a spec to a
   *     file inside it.
   *  3. `with_table` — the owning table is the LONGEST registered table location that prefixes the
   *     path, so a table nested under another table's directory resolves to the inner one.
   *  4. `with_policy` — the applicable policy is the matching one with the longest literal prefix
   *     (see PrimaryUtilities.literalPrefixOf); ties break on policy_id so a run is reproducible.
   *  5. `dated` / `in_scope` / `scoped` / `scored` — the age, in order of truthfulness: the as-of
   *     date of the engine RUN that produced the object, then the BUSINESS date in its partition
   *     value, then the file mtime. Each fallback is less truthful than the one before it, and the
   *     last one is actively misleading: re-running an engine rewrites the files, so mtime would
   *     make a 2023 vintage look produced today and keep it alive forever.
   *  6. `ranked` — `version_rank` and `version_group_total` are computed over EVERY object of the
   *     table, including those still inside retention, because neither "would this leave fewer than
   *     keepMinVersions?" (PC06) nor "what share of the table does this run remove?" (PC09) can be
   *     answered on a population already filtered down to the eligible ones. The filter comes after.
   *
   * WHAT THE FINAL ROW SET IS depends on whether anything was explicitly selected.
   *
   *  - Nothing selected — a policy-driven run: everything the retention has aged out.
   *  - Something selected — a scope-driven run: exactly that, and nothing else. When an operator
   *    names the runs to purge, or an engine configuration expands to them, going on to also collect
   *    every other aged-out object under the same root is not what anyone asked for. It is also how
   *    the file-date trap does real damage: a sibling run whose files merely look old would be swept
   *    in beside the one that was actually chosen. The policy still GOVERNS a scoped object — PC01
   *    blocks it when its retention is not reached, and default-deny still applies to one that
   *    matches no policy at all — it just no longer SELECTS.
   *
   * @param today            the reference date, injected rather than read from the clock so a run is
   *                         reproducible and the query is testable
   * @param scopeIsExclusive true when something was explicitly selected (a hand-picked path, or an
   *                         engine run), which makes that selection the whole row set
   */
  def get_purge_candidates(today: LocalDate, scopeIsExclusive: Boolean): String =
    s"""
       |WITH registered_partition AS (
       |  SELECT location_path AS path, database_name, table_name, partition_spec,
       |         table_type, external_purge
       |  FROM ${PrimaryConstants.VIEW_HIVE_CATALOG}
       |  WHERE entry_kind = '${PrimaryConstants.CATALOG_PARTITION}' AND location_path <> ''
       |),
       |registered_table AS (
       |  SELECT location_path, database_name, table_name, table_type, external_purge
       |  FROM ${PrimaryConstants.VIEW_HIVE_CATALOG}
       |  WHERE entry_kind = '${PrimaryConstants.CATALOG_TABLE}' AND location_path <> ''
       |),
       |with_partition AS (
       |  SELECT i.*
       |       , p.database_name  AS partition_database
       |       , p.table_name     AS partition_table
       |       , p.partition_spec AS partition_spec
       |       , p.table_type     AS partition_table_type
       |       , p.external_purge AS partition_external_purge
       |  FROM ${PrimaryConstants.VIEW_HDFS_INVENTORY} i
       |  LEFT JOIN registered_partition p ON i.path = p.path
       |),
       |with_table AS (
       |  SELECT w.*
       |       , t.database_name   AS table_database
       |       , t.table_name      AS table_table
       |       , t.table_type      AS table_table_type
       |       , t.external_purge  AS table_external_purge
       |       , ROW_NUMBER() OVER (
       |           PARTITION BY w.path
       |           ORDER BY LENGTH(COALESCE(t.location_path, '')) DESC, COALESCE(t.table_name, '')
       |         ) AS table_rank
       |  FROM with_partition w
       |  LEFT JOIN registered_table t
       |    ON w.path = t.location_path
       |    OR w.path LIKE CONCAT(t.location_path, '/%')
       |),
       |resolved AS (
       |  SELECT path, parent_path, object_type, root_path, size_bytes, num_files
       |       , modification_time, access_time, owner_name
       |       , COALESCE(partition_database, table_database, '')            AS database_name
       |       , COALESCE(partition_table,    table_table,    '')            AS table_name
       |       , COALESCE(partition_spec, '')                                AS partition_spec
       |       , (partition_spec IS NOT NULL)                                AS is_registered_partition
       |       , COALESCE(partition_table_type, table_table_type, '')        AS table_type
       |       , COALESCE(partition_external_purge, table_external_purge, '') AS external_purge
       |  FROM with_table
       |  WHERE table_rank = 1
       |),
       |policy_candidate AS (
       |  SELECT r.*
       |       , pol.policy_id, pol.domain, pol.date_column
       |       , pol.retention_value, pol.retention_unit, pol.keep_min_versions
       |       , pol.strategy, pol.legal_hold, pol.archive_required, pol.archive_root, pol.owner_group
       |       , ROW_NUMBER() OVER (
       |           PARTITION BY r.path
       |           ORDER BY COALESCE(pol.prefix_length, -1) DESC, COALESCE(pol.policy_id, '')
       |         ) AS policy_rank
       |  FROM resolved r
       |  LEFT JOIN ${PrimaryConstants.VIEW_PURGE_POLICY} pol
       |    ON  (pol.path_regex   = '' OR r.path       RLIKE pol.path_regex)
       |    AND (pol.table_regex  = '' OR r.table_name RLIKE pol.table_regex)
       |    AND (pol.database_name = '' OR pol.database_name = r.database_name)
       |),
       |with_policy AS (
       |  SELECT * FROM policy_candidate WHERE policy_rank = 1
       |),
       |dated AS (
       |  SELECT wp.*
       |       , CASE WHEN COALESCE(wp.date_column, '') <> ''
       |              THEN $UDF_BUSINESS_DATE(
       |                     REGEXP_EXTRACT(wp.path, CONCAT('(?i)(?:^|/)', wp.date_column, '=([^/]+)'), 1))
       |              ELSE NULL
       |         END                                        AS partition_business_date
       |       , CAST(wp.modification_time AS DATE)          AS mtime_date
       |  FROM with_policy wp
       |),
       |in_scope AS (
       |  SELECT d.path
       |       , MAX(NULLIF(sc.scope_business_date, '')) AS scope_business_date
       |  FROM dated d
       |  JOIN ${PrimaryConstants.VIEW_PURGE_SCOPE} sc
       |    ON (sc.scope_path <> '' AND d.path = sc.scope_path)
       |    OR (sc.scope_path  = ''
       |        AND sc.scope_database       = d.database_name
       |        AND sc.scope_table          = d.table_name
       |        AND sc.scope_partition_spec = d.partition_spec)
       |  GROUP BY d.path
       |),
       |scoped AS (
       |  SELECT d.*
       |       , $UDF_BUSINESS_DATE(sc.scope_business_date) AS run_business_date
       |       , (sc.path IS NOT NULL)                      AS explicitly_scoped
       |  FROM dated d
       |  LEFT JOIN in_scope sc ON d.path = sc.path
       |),
       |scored AS (
       |  SELECT s.*
       |       , COALESCE(s.run_business_date, s.partition_business_date, s.mtime_date) AS business_date
       |       , CASE WHEN s.run_business_date       IS NOT NULL THEN '${PrimaryConstants.DATE_SOURCE_RUN}'
       |              WHEN s.partition_business_date IS NOT NULL THEN '${PrimaryConstants.DATE_SOURCE_PARTITION}'
       |              ELSE '${PrimaryConstants.DATE_SOURCE_MTIME}'
       |         END                                                AS business_date_source
       |       , CASE WHEN s.retention_value IS NULL THEN NULL
       |              ELSE $UDF_RETENTION_CUTOFF(DATE '$today', s.retention_value, s.retention_unit)
       |         END                                                AS retention_cutoff
       |  FROM scoped s
       |),
       |ranked AS (
       |  SELECT s.*
       |       , DATEDIFF(DATE '$today', s.business_date)                    AS age_days
       |       , (s.retention_cutoff IS NOT NULL
       |          AND s.business_date < s.retention_cutoff)                  AS beyond_retention
       |       , COALESCE(NULLIF(CONCAT(s.database_name, '.', s.table_name), '.'), s.parent_path)
       |                                                                     AS version_group
       |       , ROW_NUMBER() OVER (
       |           PARTITION BY COALESCE(NULLIF(CONCAT(s.database_name, '.', s.table_name), '.'), s.parent_path)
       |           ORDER BY s.business_date DESC NULLS LAST, s.path DESC
       |         )                                                           AS version_rank
       |       , COUNT(*) OVER (
       |           PARTITION BY COALESCE(NULLIF(CONCAT(s.database_name, '.', s.table_name), '.'), s.parent_path)
       |         )                                                           AS version_group_total
       |  FROM scored s
       |)
       |SELECT path
       |     , parent_path
       |     , object_type
       |     , root_path
       |     , size_bytes
       |     , num_files
       |     , modification_time
       |     , access_time
       |     , owner_name
       |     , database_name
       |     , table_name
       |     , partition_spec
       |     , is_registered_partition
       |     , table_type
       |     , external_purge
       |     , policy_id
       |     , domain
       |     , date_column
       |     , retention_value
       |     , retention_unit
       |     , retention_cutoff
       |     , keep_min_versions
       |     , strategy
       |     , legal_hold
       |     , archive_required
       |     , archive_root
       |     , owner_group
       |     , business_date
       |     , business_date_source
       |     , age_days
       |     , version_group
       |     , version_rank
       |     , version_group_total
       |     , CASE WHEN beyond_retention AND explicitly_scoped THEN '${PrimaryConstants.SELECTION_BOTH}'
       |            WHEN explicitly_scoped                      THEN '${PrimaryConstants.SELECTION_SCOPE}'
       |            ELSE                                             '${PrimaryConstants.SELECTION_POLICY}'
       |       END AS selection_source
       |FROM ranked
       |WHERE ${if (scopeIsExclusive) "explicitly_scoped" else "beyond_retention OR explicitly_scoped"}
       |ORDER BY database_name, table_name, business_date, path
       |""".stripMargin
}
