package com.bnp.str.purge.reader

import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.{Config, ConfigException}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * The retention referential: for a given domain, path pattern or table, how long the data is kept,
 * how it is deleted, how many vintages must survive, and whether a legal hold applies.
 *
 * Two sources, chosen by `purge_policy.source`:
 *  - `conf`  — the rules are written inline in the application.conf TWIST generates (the P2 mode:
 *              policies can be drafted and argued about before a referential table exists);
 *  - `table` — the rules are read from a Hive table, which is where they belong once the business
 *              owns them and wants them versioned and access-controlled.
 *
 * EVERY RULE IS VALIDATED AT READ TIME, and a bad one stops the run. This is the single most
 * important decision in the file. A policy is the thing that authorises a deletion; a policy the
 * engine cannot fully interpret must never be silently dropped or half-applied, because both
 * failure modes end the same way — data disappearing under a rule nobody actually wrote. The run
 * fails, the operator fixes the conf, and nothing was at risk in the meantime.
 *
 * Matching is resolved later, in the selection SQL, on the columns produced here: `path_regex` for
 * "does this policy apply", `prefix_length` for "which of the matching policies is the most
 * specific" — see [[PrimaryUtilities.literalPrefixOf]].
 */
class PolicyReader()(implicit sparkSession: SparkSession, conf: Config) {

  import PolicyReader._

  private val log = LoggerFactory.getLogger(this.getClass)

  private val policyConfig: Config =
    conf.getConfig(s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.PURGE_POLICY}")

  def read(): DataFrame = {
    import sparkSession.implicits._

    val source = PrimaryUtilities.getStringOr(policyConfig, "source", PrimaryConstants.POLICY_SOURCE_CONF)
      .trim.toLowerCase

    val policies = source match {
      case PrimaryConstants.POLICY_SOURCE_CONF => fromConf()
      case PrimaryConstants.POLICY_SOURCE_TABLE => fromTable()
      case other => throw new IllegalArgumentException(
        s"Unknown ${PrimaryConstants.PURGE_POLICY}.source '$other'. Expected " +
          s"'${PrimaryConstants.POLICY_SOURCE_CONF}' or '${PrimaryConstants.POLICY_SOURCE_TABLE}'")
    }

    validate(policies)

    if (policies.isEmpty)
      log.warn("No retention policy loaded: a policy-driven run will select nothing, and every " +
        "explicitly scoped object will be blocked by PC01 (default-deny)")
    else
      log.info(s"${policies.size} retention polic(y|ies) loaded from $source: " +
        policies.map(p => s"${p.policy_id}(${p.retention_value} ${p.retention_unit})").mkString(", "))

    sparkSession.createDataset(policies).toDF()
  }

  /** Rules written inline as a HOCON list under `purge_policy.rules`. */
  private def fromConf(): Seq[PurgePolicy] = {
    import scala.collection.JavaConverters._

    if (!policyConfig.hasPath("rules")) return Seq.empty

    policyConfig.getConfigList("rules").asScala.toList.zipWithIndex.map { case (rule, index) =>
      try buildPolicy(
        policyId = PrimaryUtilities.getStringOr(rule, "policyId", ""),
        domain = PrimaryUtilities.getStringOr(rule, "domain", ""),
        pathPattern = PrimaryUtilities.qualifyPattern(PrimaryUtilities.getStringOr(rule, "pathPattern", "")),
        databaseName = PrimaryUtilities.getStringOr(rule, "database", ""),
        tablePattern = PrimaryUtilities.getStringOr(rule, "tablePattern", ""),
        dateColumn = PrimaryUtilities.getStringOr(rule, "dateColumn", ""),
        retentionValue = PrimaryUtilities.getIntOr(rule, "retentionValue", -1),
        retentionUnit = PrimaryUtilities.getStringOr(rule, "retentionUnit", ""),
        keepMinVersions = PrimaryUtilities.getIntOr(rule, "keepMinVersions", 0),
        strategy = PrimaryUtilities.getStringOr(rule, "strategy", PrimaryConstants.STRATEGY_TRASH),
        legalHold = PrimaryUtilities.getBooleanOr(rule, "legalHold", default = false),
        archiveRequired = PrimaryUtilities.getBooleanOr(rule, "archiveRequired", default = false),
        archiveRoot = PrimaryUtilities.getStringOr(rule, "archiveRoot", ""),
        ownerGroup = PrimaryUtilities.getStringOr(rule, "ownerGroup", ""))
      catch {
        case e: ConfigException =>
          throw new IllegalArgumentException(
            s"Invalid ${PrimaryConstants.PURGE_POLICY}.rules[$index]: ${e.getMessage}", e)
      }
    }
  }

  /** Rules read from the referential Hive table named by `purge_policy.table`. */
  private def fromTable(): Seq[PurgePolicy] = {
    val table = PrimaryUtilities.getStringOr(policyConfig, "table", "")
    require(table.nonEmpty,
      s"${PrimaryConstants.PURGE_POLICY}.source = '${PrimaryConstants.POLICY_SOURCE_TABLE}' but " +
        s"${PrimaryConstants.PURGE_POLICY}.table is not set")

    log.info(s"Reading the retention policies from $table")
    val rows = sparkSession.table(table).collect()
    val columns = sparkSession.table(table).columns.toSet

    def str(row: org.apache.spark.sql.Row, name: String, default: String = ""): String =
      if (!columns.contains(name)) default
      else Option(row.getAs[Any](name)).map(_.toString).getOrElse(default)

    def int(row: org.apache.spark.sql.Row, name: String, default: Int): Int =
      if (!columns.contains(name)) default
      else Option(row.getAs[Any](name)).map(_.toString.trim.toInt).getOrElse(default)

    def bool(row: org.apache.spark.sql.Row, name: String): Boolean =
      columns.contains(name) &&
        Option(row.getAs[Any](name)).exists(v => v.toString.trim.equalsIgnoreCase("true") || v.toString.trim == "1")

    rows.toList.map { row =>
      buildPolicy(
        policyId = str(row, "policy_id"),
        domain = str(row, "domain"),
        pathPattern = PrimaryUtilities.qualifyPattern(str(row, "path_pattern")),
        databaseName = str(row, "database_name"),
        tablePattern = str(row, "table_pattern"),
        dateColumn = str(row, "date_column"),
        retentionValue = int(row, "retention_value", -1),
        retentionUnit = str(row, "retention_unit"),
        keepMinVersions = int(row, "keep_min_versions", 0),
        strategy = str(row, "strategy", PrimaryConstants.STRATEGY_TRASH),
        legalHold = bool(row, "legal_hold"),
        archiveRequired = bool(row, "archive_required"),
        archiveRoot = str(row, "archive_root"),
        ownerGroup = str(row, "owner_group"))
    }
  }
}

object PolicyReader {

  /**
   * One retention rule, as the selection SQL sees it.
   *
   * @param path_regex     `path_pattern` compiled to an anchored regex — the "does this apply" test
   * @param prefix_length  length of the pattern's literal prefix — the "how specific is it" ranking
   * @param table_regex    `table_pattern` compiled the same way; empty pattern matches every table
   */
  final case class PurgePolicy(policy_id: String,
                               domain: String,
                               path_pattern: String,
                               path_regex: String,
                               prefix_length: Int,
                               database_name: String,
                               table_pattern: String,
                               table_regex: String,
                               date_column: String,
                               retention_value: Int,
                               retention_unit: String,
                               keep_min_versions: Int,
                               strategy: String,
                               legal_hold: Boolean,
                               archive_required: Boolean,
                               archive_root: String,
                               owner_group: String)

  private[purge] def buildPolicy(policyId: String,
                                 domain: String,
                                 pathPattern: String,
                                 databaseName: String,
                                 tablePattern: String,
                                 dateColumn: String,
                                 retentionValue: Int,
                                 retentionUnit: String,
                                 keepMinVersions: Int,
                                 strategy: String,
                                 legalHold: Boolean,
                                 archiveRequired: Boolean,
                                 archiveRoot: String,
                                 ownerGroup: String): PurgePolicy = {
    val normalizedPattern = if (pathPattern.trim.isEmpty) "" else normalizePattern(pathPattern.trim)
    PurgePolicy(
      policy_id = policyId.trim,
      domain = domain.trim,
      path_pattern = normalizedPattern,
      path_regex = if (normalizedPattern.isEmpty) "" else PrimaryUtilities.globToRegexString(normalizedPattern),
      prefix_length = PrimaryUtilities.literalPrefixOf(normalizedPattern).length,
      database_name = databaseName.trim,
      table_pattern = tablePattern.trim,
      table_regex = if (tablePattern.trim.isEmpty) "" else PrimaryUtilities.globToRegexString(tablePattern.trim),
      date_column = dateColumn.trim,
      retention_value = retentionValue,
      retention_unit = retentionUnit.trim.toUpperCase,
      keep_min_versions = keepMinVersions,
      strategy = strategy.trim.toUpperCase,
      legal_hold = legalHold,
      archive_required = archiveRequired,
      archive_root = archiveRoot.trim,
      owner_group = ownerGroup.trim)
  }

  /**
   * Canonicalise the literal part of a pattern the way every path is canonicalised, so
   * `hdfs://nn/data/rwa/` and `/data//rwa/` — each closed by a wildcard — are the same rule.
   * [[PrimaryUtilities.normalizePath]] cannot be applied to the whole pattern — it would resolve
   * the wildcard away — so only the prefix is normalised and the wildcard tail is re-attached.
   */
  private def normalizePattern(pattern: String): String = {
    val prefix = PrimaryUtilities.literalPrefixOf(pattern)
    val suffix = pattern.substring(prefix.length)
    val normalizedPrefix = PrimaryUtilities.normalizePath(prefix)
    // normalizePath drops a trailing separator that the pattern needs before its wildcard
    if (suffix.nonEmpty && prefix.endsWith("/")) normalizedPrefix + "/" + suffix
    else normalizedPrefix + suffix
  }

  /**
   * Refuse the whole referential when any rule is unusable, listing every offender at once.
   *
   * A retention we cannot interpret must stop the run, never fall through to a default — the
   * default would be "no retention", and "no retention" reads as "delete it".
   */
  private[purge] def validate(policies: Seq[PurgePolicy]): Unit = {
    val problems = policies.zipWithIndex.flatMap { case (policy, index) =>
      val label = if (policy.policy_id.nonEmpty) policy.policy_id else s"rule[$index]"
      Seq(
        if (policy.policy_id.isEmpty) Some(s"$label: policyId is missing") else None,
        if (policy.path_pattern.isEmpty && policy.table_pattern.isEmpty && policy.database_name.isEmpty)
          Some(s"$label: needs at least a pathPattern, a database or a tablePattern — " +
            "a policy matching everything is never what someone meant")
        else None,
        if (policy.retention_value < 0)
          Some(s"$label: retentionValue is missing or negative (${policy.retention_value})") else None,
        if (!PrimaryConstants.RETENTION_UNITS.contains(policy.retention_unit))
          Some(s"$label: retentionUnit '${policy.retention_unit}' is not one of " +
            PrimaryConstants.RETENTION_UNITS.mkString(", ")) else None,
        if (policy.keep_min_versions < 0)
          Some(s"$label: keepMinVersions cannot be negative (${policy.keep_min_versions})") else None,
        if (!PrimaryConstants.STRATEGIES.contains(policy.strategy))
          Some(s"$label: strategy '${policy.strategy}' is not one of " +
            PrimaryConstants.STRATEGIES.mkString(", ")) else None,
        if (policy.archive_required && policy.archive_root.isEmpty)
          Some(s"$label: archiveRequired = true but archiveRoot is empty, so PC07 could never pass")
        else None
      ).flatten
    }

    val duplicates = policies.map(_.policy_id).filter(_.nonEmpty).groupBy(identity).collect {
      case (id, occurrences) if occurrences.size > 1 => s"policyId '$id' is declared ${occurrences.size} times"
    }

    val all = problems ++ duplicates
    if (all.nonEmpty)
      throw new IllegalArgumentException(
        s"Refusing to run; ${all.size} problem(s) in the retention referential:\n" +
          all.map(p => s"  - $p").mkString("\n"))
  }
}
