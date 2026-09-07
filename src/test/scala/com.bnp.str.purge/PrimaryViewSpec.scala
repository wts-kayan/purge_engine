package com.bnp.str.purge

import com.bnp.str.purge.mapping.PrimaryMapper
import com.bnp.str.purge.reader.{CatalogReader, InventoryReader, PolicyReader, PrimaryReader}
import com.bnp.str.purge.utility.PrimaryConstants
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.LocalDate

/**
 * The candidate-selection SQL, run on synthetic inputs.
 *
 * This is the query that decides which objects are eligible for deletion, so every rule it encodes
 * is pinned here individually — the business date beating the mtime, the most specific policy
 * winning, default-deny for an unmatched object, and version_rank being counted over the whole
 * table rather than over the eligible subset.
 */
class PrimaryViewSpec extends AnyFunSuite with Matchers with SparkTestSession {

  private val today = LocalDate.of(2026, 8, 28)
  // 8 QUARTER of retention against that date -> anything before 2024-08-28 is out of retention
  private val cutoff = LocalDate.of(2024, 8, 28)

  private val freshMtime = Timestamp.valueOf("2026-08-27 10:00:00")
  private val oldMtime = Timestamp.valueOf("2020-01-15 10:00:00")

  private val config: Config = ConfigFactory.parseString(
    s"""${PrimaryConstants.APP_CONF} { ${PrimaryConstants.PURGE_MANIFEST} { queryName = "" } }""")

  // ---- builders --------------------------------------------------------------------------------

  private def unit(path: String,
                   sizeBytes: Long = 100L,
                   mtime: Timestamp = freshMtime,
                   objectType: String = PrimaryConstants.OBJECT_TYPE_LEAF_DIR): InventoryReader.PurgeUnit =
    InventoryReader.PurgeUnit(
      path = path,
      parent_path = path.substring(0, path.lastIndexOf('/')),
      object_type = objectType,
      root_path = "/data/rwa",
      depth = 1,
      size_bytes = sizeBytes,
      num_files = 1L,
      modification_time = mtime,
      access_time = None,
      owner_name = "j03627",
      group_name = "grp_str_rwa",
      permission = "rwxr-xr-x")

  private def policy(policyId: String,
                     pathPattern: String,
                     retentionValue: Int = 8,
                     retentionUnit: String = "QUARTER",
                     dateColumn: String = "as_of_date",
                     keepMinVersions: Int = 2): PolicyReader.PurgePolicy =
    PolicyReader.buildPolicy(
      policyId = policyId, domain = "RWA", pathPattern = pathPattern, databaseName = "",
      tablePattern = "", dateColumn = dateColumn, retentionValue = retentionValue,
      retentionUnit = retentionUnit, keepMinVersions = keepMinVersions,
      strategy = PrimaryConstants.STRATEGY_TRASH, legalHold = false, archiveRequired = false,
      archiveRoot = "", ownerGroup = "grp_str_rwa")

  private def partitionEntry(database: String, table: String, spec: String, location: String) =
    CatalogReader.CatalogEntry(database, table, PrimaryConstants.CATALOG_PARTITION, "EXTERNAL",
      "hive", "as_of_date", spec, location, "j03627", "FALSE")

  private def tableEntry(database: String, table: String, location: String) =
    CatalogReader.CatalogEntry(database, table, PrimaryConstants.CATALOG_TABLE, "EXTERNAL",
      "hive", "as_of_date", "", location, "j03627", "FALSE")

  private def select(units: Seq[InventoryReader.PurgeUnit],
                     policies: Seq[PolicyReader.PurgePolicy] = Seq.empty,
                     catalog: Seq[CatalogReader.CatalogEntry] = Seq.empty,
                     scope: Seq[PrimaryReader.ScopeEntry] = Seq.empty): Seq[Row] = {
    import spark.implicits._
    new PrimaryMapper(
      spark.createDataset(units).toDF(),
      spark.createDataset(catalog).toDF(),
      spark.createDataset(policies).toDF(),
      spark.createDataset(scope).toDF(),
      PrimaryConstants.PURGE_MANIFEST,
      today)(spark, config)
      .getMapping_purge
      .collect()
      .toSeq
  }

  private def paths(rows: Seq[Row]): Set[String] = rows.map(_.getAs[String]("path")).toSet

  // ---- retention -------------------------------------------------------------------------------

  test("an object beyond its retention is selected, one inside it is not") {
    val rows = select(
      Seq(unit("/data/rwa/t/as_of_date=2023-03-31"), unit("/data/rwa/t/as_of_date=2025-12-31")),
      Seq(policy("P", "/data/rwa/*")))

    paths(rows) shouldBe Set("/data/rwa/t/as_of_date=2023-03-31")
  }

  test("the cutoff is exclusive: a vintage landing exactly on it is kept") {
    val rows = select(
      Seq(unit(s"/data/rwa/t/as_of_date=$cutoff")),
      Seq(policy("P", "/data/rwa/*")))

    rows shouldBe empty
  }

  // ---- where the age comes from ----------------------------------------------------------------

  test("the age is read from the partition's business date, not from the file mtime") {
    // the whole point: re-running an engine rewrites the files, and an mtime-based age would make
    // this 2023 vintage look produced yesterday and keep it forever
    val rows = select(
      Seq(unit("/data/rwa/t/as_of_date=2023-03-31", mtime = freshMtime)),
      Seq(policy("P", "/data/rwa/*")))

    rows.size shouldBe 1
    rows.head.getAs[String]("business_date_source") shouldBe PrimaryConstants.DATE_SOURCE_PARTITION
    rows.head.getAs[java.sql.Date]("business_date").toString shouldBe "2023-03-31"
  }

  test("a quarter-notation partition value resolves to the end of the quarter") {
    val rows = select(
      Seq(unit("/data/rwa/t/as_of_date=2023Q1", mtime = freshMtime)),
      Seq(policy("P", "/data/rwa/*")))

    rows.head.getAs[java.sql.Date]("business_date").toString shouldBe "2023-03-31"
  }

  test("without a usable partition value the age falls back to the modification time") {
    val rows = select(
      Seq(unit("/data/rwa/t/some_folder", mtime = oldMtime)),
      Seq(policy("P", "/data/rwa/*")))

    rows.size shouldBe 1
    rows.head.getAs[String]("business_date_source") shouldBe PrimaryConstants.DATE_SOURCE_MTIME
    rows.head.getAs[Int]("age_days") should be > 2000
  }

  test("a fresh file with no business date is kept, since its mtime is all we know") {
    select(Seq(unit("/data/rwa/t/some_folder", mtime = freshMtime)), Seq(policy("P", "/data/rwa/*"))) shouldBe empty
  }

  // ---- which policy applies --------------------------------------------------------------------

  test("the matching policy with the longest literal prefix wins") {
    val rows = select(
      Seq(unit("/data/rwa/ts_ead_fwd/as_of_date=2023-03-31")),
      Seq(policy("P-DOMAIN", "/data/rwa/*", retentionValue = 2, retentionUnit = "YEAR"),
        policy("P-TABLE", "/data/rwa/ts_ead_fwd/*", retentionValue = 8, retentionUnit = "QUARTER")))

    rows.size shouldBe 1
    rows.head.getAs[String]("policy_id") shouldBe "P-TABLE"
    rows.head.getAs[String]("retention_unit") shouldBe "QUARTER"
  }

  test("a policy that does not match the path is not applied") {
    select(
      Seq(unit("/data/ifrs9/t/as_of_date=2023-03-31")),
      Seq(policy("P", "/data/rwa/*"))) shouldBe empty
  }

  // ---- default-deny ------------------------------------------------------------------------------

  test("an object matching no policy is absent from a policy-driven run") {
    select(Seq(unit("/data/rwa/t/as_of_date=2010-03-31")), Seq.empty) shouldBe empty
  }

  test("an explicitly selected object with no policy is carried through with a null retention") {
    // it must reach the manifest so PC01 can block it and the report can say why — silently
    // dropping it would hide the fact that someone asked for it
    val rows = select(
      Seq(unit("/data/rwa/t/as_of_date=2010-03-31")),
      Seq.empty,
      scope = Seq(PrimaryReader.ScopeEntry("/data/rwa/t/as_of_date=2010-03-31", "", "", "")))

    rows.size shouldBe 1
    rows.head.getAs[String]("selection_source") shouldBe PrimaryConstants.SELECTION_SCOPE
    rows.head.isNullAt(rows.head.fieldIndex("policy_id")) shouldBe true
    rows.head.isNullAt(rows.head.fieldIndex("retention_cutoff")) shouldBe true
  }

  test("an object that is both aged out and explicitly selected says so") {
    val rows = select(
      Seq(unit("/data/rwa/t/as_of_date=2023-03-31")),
      Seq(policy("P", "/data/rwa/*")),
      scope = Seq(PrimaryReader.ScopeEntry("/data/rwa/t/as_of_date=2023-03-31", "", "", "")))

    rows.head.getAs[String]("selection_source") shouldBe PrimaryConstants.SELECTION_BOTH
  }

  test("a still-fresh object can be selected explicitly") {
    val rows = select(
      Seq(unit("/data/rwa/t/as_of_date=2025-12-31")),
      Seq(policy("P", "/data/rwa/*")),
      scope = Seq(PrimaryReader.ScopeEntry("/data/rwa/t/as_of_date=2025-12-31", "", "", "")))

    rows.size shouldBe 1
    rows.head.getAs[String]("selection_source") shouldBe PrimaryConstants.SELECTION_SCOPE
  }

  test("a partition can be selected by its metastore identity rather than by path") {
    val rows = select(
      Seq(unit("/data/rwa/ts_ead_fwd/as_of_date=2025-12-31")),
      Seq(policy("P", "/data/rwa/*")),
      catalog = Seq(partitionEntry("dbiris", "ts_ead_fwd", "as_of_date=2025-12-31",
        "/data/rwa/ts_ead_fwd/as_of_date=2025-12-31")),
      scope = Seq(PrimaryReader.ScopeEntry("", "dbiris", "ts_ead_fwd", "as_of_date=2025-12-31")))

    rows.size shouldBe 1
    rows.head.getAs[String]("selection_source") shouldBe PrimaryConstants.SELECTION_SCOPE
  }

  // ---- an explicit selection is exclusive -----------------------------------------------------------

  test("naming what to purge stops the policy from collecting anything else") {
    // the trap this closes: a sibling run whose FILES merely look old being swept in beside the one
    // that was actually chosen
    val chosen = "/data/rwa/t/as_of_date=2023-03-31"
    val rows = select(
      Seq(unit(chosen), unit("/data/rwa/t/as_of_date=2023-06-30", mtime = oldMtime)),
      Seq(policy("P", "/data/rwa/*")),
      scope = Seq(PrimaryReader.ScopeEntry(chosen, "", "", "")))

    paths(rows) shouldBe Set(chosen)
  }

  test("with nothing selected the policy is what collects candidates") {
    val rows = select(
      Seq(unit("/data/rwa/t/as_of_date=2023-03-31"), unit("/data/rwa/t/as_of_date=2023-06-30")),
      Seq(policy("P", "/data/rwa/*")))

    rows.size shouldBe 2
  }

  test("the run's as-of date beats both the partition value and the file date") {
    // a projection partition is a run id, not a date; the run's own as-of quarter is its true age
    val runPath = "/data/rwa/term_structure/runId=9df8cf3a"
    val rows = select(
      Seq(unit(runPath, mtime = freshMtime)),
      Seq(policy("P", "/data/rwa/*", dateColumn = "as_of_date")),
      scope = Seq(PrimaryReader.ScopeEntry(runPath, "", "", "", "2023-09-30")))

    rows.size shouldBe 1
    rows.head.getAs[String]("business_date_source") shouldBe PrimaryConstants.DATE_SOURCE_RUN
    rows.head.getAs[java.sql.Date]("business_date").toString shouldBe "2023-09-30"
  }

  test("a scoped object with no run date still falls back to the file date") {
    val path = "/data/rwa/t/some_folder"
    val rows = select(Seq(unit(path, mtime = oldMtime)), Seq(policy("P", "/data/rwa/*")),
      scope = Seq(PrimaryReader.ScopeEntry(path, "", "", "")))

    rows.head.getAs[String]("business_date_source") shouldBe PrimaryConstants.DATE_SOURCE_MTIME
  }

  // ---- the metastore ------------------------------------------------------------------------------

  test("a path that IS a registered partition carries its database, table and spec") {
    val rows = select(
      Seq(unit("/data/rwa/ts_ead_fwd/as_of_date=2023-03-31")),
      Seq(policy("P", "/data/rwa/*")),
      catalog = Seq(partitionEntry("dbiris", "ts_ead_fwd", "as_of_date=2023-03-31",
        "/data/rwa/ts_ead_fwd/as_of_date=2023-03-31")))

    val row = rows.head
    row.getAs[Boolean]("is_registered_partition") shouldBe true
    row.getAs[String]("database_name") shouldBe "dbiris"
    row.getAs[String]("partition_spec") shouldBe "as_of_date=2023-03-31"
  }

  test("an unregistered path under a table location is attributed to that table, not marked a partition") {
    val rows = select(
      Seq(unit("/data/rwa/ts_ead_fwd/as_of_date=2023-03-31")),
      Seq(policy("P", "/data/rwa/*")),
      catalog = Seq(tableEntry("dbiris", "ts_ead_fwd", "/data/rwa/ts_ead_fwd")))

    val row = rows.head
    row.getAs[Boolean]("is_registered_partition") shouldBe false
    row.getAs[String]("table_name") shouldBe "ts_ead_fwd"
  }

  test("the owning table is the longest table location that prefixes the path") {
    val rows = select(
      Seq(unit("/data/rwa/outer/inner/as_of_date=2023-03-31")),
      Seq(policy("P", "/data/rwa/*")),
      catalog = Seq(tableEntry("dbiris", "outer", "/data/rwa/outer"),
        tableEntry("dbiris", "inner", "/data/rwa/outer/inner")))

    rows.head.getAs[String]("table_name") shouldBe "inner"
  }

  test("a table whose name merely prefixes another does not capture its paths") {
    val rows = select(
      Seq(unit("/data/rwa/ts_ead_fwd_old/as_of_date=2023-03-31")),
      Seq(policy("P", "/data/rwa/*")),
      catalog = Seq(tableEntry("dbiris", "ts_ead_fwd", "/data/rwa/ts_ead_fwd")))

    rows.head.getAs[String]("table_name") shouldBe ""
  }

  // ---- version_rank -------------------------------------------------------------------------------

  test("version_rank is counted over EVERY vintage, not only the eligible ones") {
    // keepMinVersions asks "would this leave fewer than N?" — a rank computed on the already
    // filtered set would answer that question about the wrong population
    val rows = select(
      Seq(unit("/data/rwa/t/as_of_date=2026-06-30"),  // newest, inside retention
        unit("/data/rwa/t/as_of_date=2025-12-31"),    // inside retention
        unit("/data/rwa/t/as_of_date=2023-06-30"),    // out of retention
        unit("/data/rwa/t/as_of_date=2023-03-31")),   // out of retention, oldest
      Seq(policy("P", "/data/rwa/*")))

    val byPath = rows.map(r => r.getAs[String]("path") -> r.getAs[Int]("version_rank")).toMap
    byPath.size shouldBe 2
    byPath("/data/rwa/t/as_of_date=2023-06-30") shouldBe 3
    byPath("/data/rwa/t/as_of_date=2023-03-31") shouldBe 4
  }

  test("vintages of different tables are ranked independently") {
    val rows = select(
      Seq(unit("/data/rwa/t1/as_of_date=2023-03-31"), unit("/data/rwa/t2/as_of_date=2023-03-31")),
      Seq(policy("P", "/data/rwa/*")))

    rows.map(_.getAs[Int]("version_rank")).toSet shouldBe Set(1)
    rows.map(_.getAs[String]("version_group")).toSet.size shouldBe 2
  }

  // ---- shape ---------------------------------------------------------------------------------------

  test("an empty inventory yields an empty candidate set rather than failing") {
    select(Seq.empty, Seq(policy("P", "/data/rwa/*"))) shouldBe empty
  }

  test("the candidate set carries everything the controls will need at P3") {
    val rows = select(
      Seq(unit("/data/rwa/t/as_of_date=2023-03-31")),
      Seq(policy("P", "/data/rwa/*")))

    val fields = rows.head.schema.fieldNames.toSet
    Seq("path", "size_bytes", "num_files", "modification_time", "access_time", "owner_name",
      "database_name", "table_name", "partition_spec", "is_registered_partition", "external_purge",
      "policy_id", "retention_value", "retention_unit", "retention_cutoff", "keep_min_versions",
      "strategy", "legal_hold", "archive_required", "archive_root", "owner_group",
      "business_date", "business_date_source", "age_days", "version_group", "version_rank",
      "selection_source").foreach(fields should contain(_))
  }
}
