package com.bnp.str.purge

import com.bnp.str.purge.control._
import com.bnp.str.purge.mapping.{ManifestView, PrimaryMapper}
import com.bnp.str.purge.reader.{CatalogReader, InventoryReader, PolicyReader, PrimaryReader}
import com.bnp.str.purge.utility.PrimaryConstants
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.LocalDate

/**
 * The thirteen controls, each pinned individually.
 *
 * Two properties are checked over and over because they are what makes the report worth reading:
 * a control that could not run reports UNKNOWN and never PASS, and a blocking hit removes the
 * object from what may be deleted no matter what else the row says.
 */
class PurgeControlMapperSpec extends AnyFunSuite with Matchers with SparkTestSession {

  private val today = LocalDate.of(2026, 8, 28)
  private val allowedRoot = "/data/rwa"
  private val freshMtime = Timestamp.valueOf("2026-08-27 10:00:00")

  /** A run started moments ago — one PC04 should still consider in flight. */
  private def justNow: Timestamp = new Timestamp(System.currentTimeMillis() - 60000L)

  // ---- fixtures --------------------------------------------------------------------------------

  private def unit(path: String,
                   sizeBytes: Long = 100L,
                   accessTime: Option[Timestamp] = None): InventoryReader.PurgeUnit =
    InventoryReader.PurgeUnit(
      path = path,
      parent_path = path.substring(0, path.lastIndexOf('/')),
      object_type = PrimaryConstants.OBJECT_TYPE_LEAF_DIR,
      root_path = allowedRoot,
      depth = 1,
      size_bytes = sizeBytes,
      num_files = 1L,
      modification_time = freshMtime,
      access_time = accessTime,
      owner_name = "j03627",
      group_name = "grp_str_rwa",
      permission = "rwxr-xr-x")

  private def policy(retentionValue: Int = 8,
                     keepMinVersions: Int = 0,
                     legalHold: Boolean = false,
                     strategy: String = PrimaryConstants.STRATEGY_TRASH,
                     archiveRequired: Boolean = false,
                     archiveRoot: String = "",
                     ownerGroup: String = ""): PolicyReader.PurgePolicy =
    PolicyReader.buildPolicy(
      policyId = "P", domain = "RWA", pathPattern = s"$allowedRoot/*", databaseName = "",
      tablePattern = "", dateColumn = "as_of_date", retentionValue = retentionValue,
      retentionUnit = "QUARTER", keepMinVersions = keepMinVersions, strategy = strategy,
      legalHold = legalHold, archiveRequired = archiveRequired, archiveRoot = archiveRoot,
      ownerGroup = ownerGroup)

  /** The conf blocks the controls read, with everything a test does not care about left default. */
  private def conf(controls: String = "", guard: String = "", request: String = ""): Config =
    ConfigFactory.parseString(
      s"""${PrimaryConstants.APP_CONF} {
         |  scan { allowedRoots = [ "$allowedRoot" ], roots = [ "$allowedRoot" ], databases = [] }
         |  request { id = "PRG-1", requester = "j03627", asOfDate = "$today" $request }
         |  controls { htmlPath = "" $controls }
         |  guard { $guard }
         |  ${PrimaryConstants.PURGE_MANIFEST} { queryName = "" }
         |}""".stripMargin)

  private def manifestOf(units: Seq[InventoryReader.PurgeUnit],
                         policies: Seq[PolicyReader.PurgePolicy],
                         catalog: Seq[CatalogReader.CatalogEntry],
                         scope: Seq[PrimaryReader.ScopeEntry],
                         config: Config): DataFrame = {
    import spark.implicits._
    val candidates = new PrimaryMapper(
      spark.createDataset(units).toDF(),
      spark.createDataset(catalog).toDF(),
      spark.createDataset(policies).toDF(),
      spark.createDataset(scope).toDF(),
      PrimaryConstants.PURGE_MANIFEST,
      today)(spark, config).getMapping_purge
    ManifestView.build(candidates, "run-1", "PRG-1", today, "fp")(spark)
  }

  private def run(units: Seq[InventoryReader.PurgeUnit],
                  policies: Seq[PolicyReader.PurgePolicy] = Seq(policy()),
                  catalog: Seq[CatalogReader.CatalogEntry] = Seq.empty,
                  scope: Seq[PrimaryReader.ScopeEntry] = Seq.empty,
                  config: Config = conf(),
                  runHistory: Option[DataFrame] = None,
                  protectedPaths: Seq[String] = Seq.empty): ControlOutcome = {
    import spark.implicits._
    val history = runHistory.getOrElse(spark.emptyDataset[PrimaryReader.RunHistoryEntry].toDF())
    new PurgeControlMapper(CheckConfig.from(config)(spark), history, protectedPaths)(spark)
      .apply(manifestOf(units, policies, catalog, scope, config), allowedRoot, "run-1", "PRG-1", "fp")
  }

  private def resultOf(outcome: ControlOutcome, rule: PurgeRule): CheckRuleResult =
    outcome.report.results.find(_.rule == rule).getOrElse(fail(s"${rule.id} missing from the report"))

  private def decisions(outcome: ControlOutcome): Seq[(String, String, Seq[String])] =
    outcome.manifest
      .select("path", "decision", "controls_ko")
      .collect()
      .map(r => (r.getString(0), r.getString(1), r.getSeq[String](2)))
      .toSeq

  private val agedOut = "/data/rwa/t/as_of_date=2023-03-31"

  // ---- the happy path --------------------------------------------------------------------------

  test("an object no control fires on is DELETE") {
    val outcome = run(Seq(unit(agedOut)))

    decisions(outcome) shouldBe Seq((agedOut, PrimaryConstants.DECISION_DELETE, Seq.empty))
    outcome.report.verdict shouldBe "CLEAR"
    outcome.report.toDelete shouldBe 1L
  }

  // ---- PC01 retention --------------------------------------------------------------------------

  test("PC01 does not fire on an object the policy aged out") {
    resultOf(run(Seq(unit(agedOut))), PurgeRule.RetentionNotReached).status shouldBe "PASS"
  }

  test("an explicitly selected object that matched no policy is deletable, and PC15 says so") {
    // the business owns its data and decides what to purge (Q3), so what protects an object is that
    // nobody named it - not that a policy failed to authorise it
    val path = "/data/rwa/t/as_of_date=2010-01-31"
    val outcome = run(Seq(unit(path)), policies = Seq.empty,
      scope = Seq(PrimaryReader.ScopeEntry(path, "", "", "")))

    resultOf(outcome, PurgeRule.RetentionNotReached).status shouldBe "PASS"
    resultOf(outcome, PurgeRule.NoRetentionPolicy).total shouldBe 1L
    decisions(outcome).head._2 shouldBe PrimaryConstants.DECISION_WARN
  }

  test("blockWithoutPolicy restores the stricter default-deny reading") {
    val path = "/data/rwa/t/as_of_date=2010-01-31"
    val outcome = run(Seq(unit(path)), policies = Seq.empty,
      scope = Seq(PrimaryReader.ScopeEntry(path, "", "", "")),
      config = conf(controls = ", retention_not_reached { blockWithoutPolicy = true }"))

    resultOf(outcome, PurgeRule.RetentionNotReached).total shouldBe 1L
    decisions(outcome).head._2 shouldBe PrimaryConstants.DECISION_BLOCKED
  }

  test("PC15 does not fire when a policy governs the object") {
    resultOf(run(Seq(unit(agedOut))), PurgeRule.NoRetentionPolicy).status shouldBe "PASS"
  }

  test("a whole engine run is deletable with no retention referential at all") {
    // the shape of a real projection purge after Q3: no policies anywhere, the run is the scope
    val runUuid = "9df8cf3a-c2fa-4bfd-9068-aa6587ba84cf"
    val paths = Seq("term_structure", "projected_dr", "lgd")
      .map(t => s"/data/rwa/$t/runId=$runUuid")
    val outcome = run(paths.map(unit(_)), policies = Seq.empty,
      scope = paths.map(p => PrimaryReader.ScopeEntry(p, "", "", "", "2023-09-30")))

    outcome.report.blocked shouldBe 0L
    outcome.report.candidates shouldBe 3L
    decisions(outcome).map(_._2).distinct shouldBe Seq(PrimaryConstants.DECISION_WARN)
  }

  test("PC01 blocks an explicitly selected object that is still inside its retention") {
    val path = "/data/rwa/t/as_of_date=2026-06-30"
    val outcome = run(Seq(unit(path)), scope = Seq(PrimaryReader.ScopeEntry(path, "", "", "")))

    resultOf(outcome, PurgeRule.RetentionNotReached).total shouldBe 1L
    resultOf(outcome, PurgeRule.RetentionNotReached).findings.head.value should include("cutoff")
  }

  // ---- PC02 legal hold -------------------------------------------------------------------------

  test("PC02 blocks an object whose policy carries a legal hold") {
    val outcome = run(Seq(unit(agedOut)), policies = Seq(policy(legalHold = true)))

    resultOf(outcome, PurgeRule.LegalHold).total shouldBe 1L
    decisions(outcome).head._2 shouldBe PrimaryConstants.DECISION_BLOCKED
  }

  // ---- PC03 path containment -------------------------------------------------------------------

  test("PC03 blocks a path outside the allowed roots") {
    // reachable when a policy pattern is wider than the scan roots; the control is the backstop
    val outside = "/data/ifrs9/t/as_of_date=2023-03-31"
    val wide = PolicyReader.buildPolicy("P", "RWA", "/data/*", "", "", "as_of_date", 8, "QUARTER",
      0, PrimaryConstants.STRATEGY_TRASH, legalHold = false, archiveRequired = false, "", "")
    val outcome = run(Seq(unit(outside)), policies = Seq(wide))

    resultOf(outcome, PurgeRule.PathOutOfScope).total shouldBe 1L
    resultOf(outcome, PurgeRule.PathOutOfScope).findings.head.value should include("outside the allowed roots")
    decisions(outcome).head._2 shouldBe PrimaryConstants.DECISION_BLOCKED
  }

  test("PC03 cannot be switched off") {
    val outcome = run(Seq(unit("/data/ifrs9/t/as_of_date=2023-03-31")),
      policies = Seq(PolicyReader.buildPolicy("P", "RWA", "/data/*", "", "", "as_of_date", 8,
        "QUARTER", 0, PrimaryConstants.STRATEGY_TRASH, false, false, "", "")),
      config = conf(controls = ", path_out_of_scope { enabled = false }"))

    // PurgeGuard re-checks it in code at P4; a report that omitted it would understate the run
    resultOf(outcome, PurgeRule.PathOutOfScope).enabled shouldBe true
    resultOf(outcome, PurgeRule.PathOutOfScope).total shouldBe 1L
  }

  // ---- PC04 active run -------------------------------------------------------------------------

  test("PC04 reports UNKNOWN when run_history is empty, never PASS") {
    // "no job is running" and "we cannot tell whether a job is running" are different facts
    val result = resultOf(run(Seq(unit(agedOut))), PurgeRule.ActiveRun)

    result.status shouldBe "UNKNOWN"
    result.notEvaluated shouldBe Some(NotEvaluated.NO_DATA)
  }

  test("PC04 blocks an object under a job with no end date") {
    import spark.implicits._
    val history = Seq(PrimaryReader.RunHistoryEntry(
      "r1", "tseadfwd", "RUNNING", justNow, None, Some("t"))).toDF()
    val outcome = run(Seq(unit(agedOut)), runHistory = Some(history))

    resultOf(outcome, PurgeRule.ActiveRun).total shouldBe 1L
    decisions(outcome).head._2 shouldBe PrimaryConstants.DECISION_BLOCKED
  }

  test("PC04 fires on a run that has not finished, whatever word its engine uses for that") {
    // dbprojection.run_history stores `succeeded` in lower case and nothing documents its in-flight
    // value; matching a status literal is a guess, and a wrong guess means this control never fires
    import spark.implicits._
    val history = Seq(PrimaryReader.RunHistoryEntry(
      "r1", "projection", "en cours", justNow, None, Some("t"))).toDF()

    resultOf(run(Seq(unit(agedOut)), runHistory = Some(history)), PurgeRule.ActiveRun).total shouldBe 1L
  }

  test("PC04 presumes a long-unfinished row is dead rather than running") {
    // a row left without an end by a killed JVM would otherwise protect its run for ever, and the
    // runs nobody finished are exactly the ones people want to purge
    import spark.implicits._
    val ancient = new Timestamp(System.currentTimeMillis() - 100L * 3600L * 1000L)
    val history = Seq(PrimaryReader.RunHistoryEntry(
      "r1", "projection", "running", ancient, None, Some("t"))).toDF()

    resultOf(run(Seq(unit(agedOut)), runHistory = Some(history)), PurgeRule.ActiveRun).status shouldBe "PASS"
  }

  test("PC04 passes on a run that finished long ago, lower-case status and all") {
    import spark.implicits._
    val history = Seq(PrimaryReader.RunHistoryEntry(
      "r1", "projection", "succeeded", Timestamp.valueOf("2026-02-03 15:51:44"),
      Some(Timestamp.valueOf("2026-02-03 16:00:38")), Some("t"))).toDF()

    resultOf(run(Seq(unit(agedOut)), runHistory = Some(history)), PurgeRule.ActiveRun).status shouldBe "PASS"
  }

  test("PC04 passes when run_history holds only finished runs on other tables") {
    import spark.implicits._
    val history = Seq(
      PrimaryReader.RunHistoryEntry("r1", "addons", "SUCCESS",
        Timestamp.valueOf("2020-01-01 00:00:00"),
        Some(Timestamp.valueOf("2020-01-01 01:00:00")), Some("other_table"))).toDF()

    resultOf(run(Seq(unit(agedOut)), runHistory = Some(history)), PurgeRule.ActiveRun).status shouldBe "PASS"
  }

  // ---- PC05 dependencies -----------------------------------------------------------------------

  test("PC05 reports UNKNOWN when no dependency referential is configured") {
    resultOf(run(Seq(unit(agedOut))), PurgeRule.DownstreamDependency).status shouldBe "UNKNOWN"
  }

  test("PC05 reports UNKNOWN when the referential cannot be read, rather than passing") {
    val outcome = run(Seq(unit(agedOut)),
      config = conf(controls = ", downstream_dependency { referential = \"no_such_db.no_such_table\" }"))

    resultOf(outcome, PurgeRule.DownstreamDependency).status shouldBe "UNKNOWN"
  }

  // ---- PC06 minimum versions -------------------------------------------------------------------

  test("PC06 blocks the newest vintages the policy requires to survive") {
    val outcome = run(
      Seq(unit("/data/rwa/t/as_of_date=2023-06-30"), unit("/data/rwa/t/as_of_date=2023-03-31")),
      policies = Seq(policy(keepMinVersions = 1)))

    val blocked = decisions(outcome).filter(_._2 == PrimaryConstants.DECISION_BLOCKED)
    blocked.map(_._1) shouldBe Seq("/data/rwa/t/as_of_date=2023-06-30")
    resultOf(outcome, PurgeRule.MinVersionsKept).total shouldBe 1L
  }

  test("PC06 counts the rank over every vintage, including those still inside retention") {
    // two fresh vintages already satisfy keepMinVersions = 2, so both aged-out ones may go
    val outcome = run(
      Seq(unit("/data/rwa/t/as_of_date=2026-06-30"), unit("/data/rwa/t/as_of_date=2025-12-31"),
        unit("/data/rwa/t/as_of_date=2023-06-30"), unit("/data/rwa/t/as_of_date=2023-03-31")),
      policies = Seq(policy(keepMinVersions = 2)))

    resultOf(outcome, PurgeRule.MinVersionsKept).status shouldBe "PASS"
    decisions(outcome).count(_._2 == PrimaryConstants.DECISION_DELETE) shouldBe 2
  }

  test("PC06 does not fire when the policy asks for no minimum") {
    resultOf(run(Seq(unit(agedOut))), PurgeRule.MinVersionsKept).status shouldBe "PASS"
  }

  // ---- PC07 archive ----------------------------------------------------------------------------

  test("PC07 blocks a HARD delete with no archive copy") {
    val outcome = run(Seq(unit(agedOut)),
      policies = Seq(policy(strategy = PrimaryConstants.STRATEGY_HARD, archiveRequired = true,
        archiveRoot = "/archive/rwa")))

    resultOf(outcome, PurgeRule.NoArchive).total shouldBe 1L
    decisions(outcome).head._2 shouldBe PrimaryConstants.DECISION_BLOCKED
  }

  test("PC07 is not evaluated for a TRASH delete, which is recoverable on its own") {
    val outcome = run(Seq(unit(agedOut)),
      policies = Seq(policy(archiveRequired = true, archiveRoot = "/archive/rwa")))

    resultOf(outcome, PurgeRule.NoArchive).status shouldBe "PASS"
  }

  // ---- PC08 authorisation ----------------------------------------------------------------------

  test("PC08 reports UNKNOWN when TWIST supplied no group list") {
    resultOf(run(Seq(unit(agedOut)), policies = Seq(policy(ownerGroup = "grp_str_rwa"))),
      PurgeRule.NotAuthorized).status shouldBe "UNKNOWN"
  }

  test("PC08 blocks when the requester is not in the owning group") {
    val outcome = run(Seq(unit(agedOut)),
      policies = Seq(policy(ownerGroup = "grp_str_rwa")),
      config = conf(request = ", requesterGroups = [ \"grp_str_ifrs9\" ]"))

    resultOf(outcome, PurgeRule.NotAuthorized).total shouldBe 1L
    decisions(outcome).head._2 shouldBe PrimaryConstants.DECISION_BLOCKED
  }

  test("PC08 passes when the requester is in the owning group") {
    val outcome = run(Seq(unit(agedOut)),
      policies = Seq(policy(ownerGroup = "grp_str_rwa")),
      config = conf(request = ", requesterGroups = [ \"grp_str_rwa\", \"grp_str_ifrs9\" ]"))

    resultOf(outcome, PurgeRule.NotAuthorized).status shouldBe "PASS"
  }

  // ---- PC09 blast radius -----------------------------------------------------------------------

  test("PC09 blocks every candidate when the object ceiling is crossed") {
    // a purge that is defensible object by object can still be a mistake taken as a whole
    val outcome = run(
      Seq(unit("/data/rwa/t/as_of_date=2023-06-30"), unit("/data/rwa/t/as_of_date=2023-03-31")),
      config = conf(guard = "maxObjects = 1"))

    resultOf(outcome, PurgeRule.BlastRadius).total shouldBe 2L
    decisions(outcome).map(_._2).distinct shouldBe Seq(PrimaryConstants.DECISION_BLOCKED)
  }

  test("PC09 blocks every candidate when the byte ceiling is crossed") {
    val outcome = run(Seq(unit(agedOut, sizeBytes = 5000L)), config = conf(guard = "maxBytes = 100"))
    resultOf(outcome, PurgeRule.BlastRadius).total shouldBe 1L
  }

  test("PC09 blocks only the tables losing more than the allowed share") {
    val outcome = run(
      Seq(unit("/data/rwa/big/as_of_date=2026-06-30"), unit("/data/rwa/big/as_of_date=2026-03-31"),
        unit("/data/rwa/big/as_of_date=2023-03-31"), unit("/data/rwa/small/as_of_date=2023-03-31")),
      config = conf(guard = "maxPercentOfTable = 50"))

    // small/ loses its only vintage (100%), big/ loses one of three (33%)
    val blocked = decisions(outcome).filter(_._2 == PrimaryConstants.DECISION_BLOCKED).map(_._1)
    blocked shouldBe Seq("/data/rwa/small/as_of_date=2023-03-31")
  }

  test("PC09 passes under the ceilings") {
    resultOf(run(Seq(unit(agedOut)), config = conf(guard = "maxObjects = 10, maxBytes = 100000")),
      PurgeRule.BlastRadius).status shouldBe "PASS"
  }

  // ---- PC14 shared inputs ------------------------------------------------------------------------

  test("PC14 blocks an object a run declared as an input") {
    // scoped explicitly, which is the only way an input reaches the controls at all: a scope drawn
    // wider than someone realised is exactly the case PC14 exists for
    val input = "/data/rwa/models/model_ifrs9.csv"
    val outcome = run(Seq(unit(input)), scope = Seq(PrimaryReader.ScopeEntry(input, "", "", "", "2020-01-31")),
      protectedPaths = Seq(input))

    resultOf(outcome, PurgeRule.SharedInput).total shouldBe 1L
    decisions(outcome).head._2 shouldBe PrimaryConstants.DECISION_BLOCKED
  }

  test("PC14 blocks a directory that CONTAINS a shared input") {
    // deleting the folder a shared model sits in destroys it just as thoroughly as deleting the file
    val outcome = run(Seq(unit("/data/rwa/models")),
      scope = Seq(PrimaryReader.ScopeEntry("/data/rwa/models", "", "", "", "2020-01-31")),
      protectedPaths = Seq("/data/rwa/models/model_ifrs9.csv"))

    resultOf(outcome, PurgeRule.SharedInput).total shouldBe 1L
  }

  test("PC14 does not fire on a sibling whose name merely starts the same way") {
    val outcome = run(Seq(unit(agedOut)),
      protectedPaths = Seq("/data/rwa/t/as_of_date=2023-03-31-old"))
    resultOf(outcome, PurgeRule.SharedInput).status shouldBe "PASS"
  }

  test("PC14 passes when nothing is protected") {
    resultOf(run(Seq(unit(agedOut))), PurgeRule.SharedInput).status shouldBe "PASS"
  }

  // ---- PC04 against an engine run history ---------------------------------------------------------

  test("PC04 blocks a partition of a projection run that is still RUNNING") {
    // the engine's own history names the run; its partition directory is the token in the path
    import spark.implicits._
    val runUuid = "9df8cf3a-c2fa-4bfd-9068-aa6587ba84cf"
    val history = Seq(PrimaryReader.RunHistoryEntry(
      runUuid, "projection", "RUNNING", justNow, None, Some(s"runId=$runUuid"))).toDF()

    val path = s"/data/rwa/term_structure/runId=$runUuid"
    val outcome = run(Seq(unit(path)), scope = Seq(PrimaryReader.ScopeEntry(path, "", "", "", "2023-03-31")),
      runHistory = Some(history))

    resultOf(outcome, PurgeRule.ActiveRun).total shouldBe 1L
    decisions(outcome).head._2 shouldBe PrimaryConstants.DECISION_BLOCKED
  }

  // ---- PC10 / PC11 warnings --------------------------------------------------------------------

  test("PC10 flags a recently read object without blocking it") {
    val outcome = run(Seq(unit(agedOut, accessTime = Some(Timestamp.valueOf("2026-08-20 09:00:00")))))

    resultOf(outcome, PurgeRule.RecentlyAccessed).total shouldBe 1L
    decisions(outcome).head._2 shouldBe PrimaryConstants.DECISION_WARN
    outcome.report.verdict shouldBe "WARNINGS"
  }

  test("PC10 does not fire when the cluster does not maintain atime") {
    // absent atime is unknown, and unknown is not recent
    resultOf(run(Seq(unit(agedOut, accessTime = None))), PurgeRule.RecentlyAccessed).status shouldBe "PASS"
  }

  test("PC11 flags a registered partition so the executor drops it too") {
    val outcome = run(Seq(unit(agedOut)),
      catalog = Seq(CatalogReader.CatalogEntry("dbiris", "t", PrimaryConstants.CATALOG_PARTITION,
        "EXTERNAL", "hive", "as_of_date", "as_of_date=2023-03-31", agedOut, "j03627", "FALSE")))

    resultOf(outcome, PurgeRule.HiveMetadataOrphan).total shouldBe 1L
    decisions(outcome).head._2 shouldBe PrimaryConstants.DECISION_WARN
  }

  // ---- PC12 / PC13 execution phase -------------------------------------------------------------

  test("PC12 and PC13 are reported NOT EVALUATED during a simulation") {
    val outcome = run(Seq(unit(agedOut)))

    resultOf(outcome, PurgeRule.ManifestDrift).status shouldBe "NOT EVALUATED"
    resultOf(outcome, PurgeRule.AlreadyAbsent).notEvaluated shouldBe Some(NotEvaluated.EXECUTE_PHASE)
  }

  // ---- precedence and the disabled case ---------------------------------------------------------

  test("a blocking hit wins over a warning on the same object") {
    val outcome = run(Seq(unit(agedOut, accessTime = Some(Timestamp.valueOf("2026-08-20 09:00:00")))),
      policies = Seq(policy(legalHold = true)))

    decisions(outcome).head._2 shouldBe PrimaryConstants.DECISION_BLOCKED
  }

  test("a switched-off control reports SKIPPED, not PASS") {
    val outcome = run(Seq(unit(agedOut, accessTime = Some(Timestamp.valueOf("2026-08-20 09:00:00")))),
      config = conf(controls = ", recently_accessed { enabled = false }"))

    resultOf(outcome, PurgeRule.RecentlyAccessed).status shouldBe "SKIPPED"
    decisions(outcome).head._2 shouldBe PrimaryConstants.DECISION_DELETE
  }

  test("controls.enabled = false leaves every object at CANDIDATE, which deletes nothing") {
    val outcome = run(Seq(unit(agedOut)), config = conf(controls = ", enabled = false"))

    decisions(outcome).head._2 shouldBe PrimaryConstants.DECISION_CANDIDATE
    outcome.report.results.map(_.status).distinct shouldBe Seq("SKIPPED")
    outcome.report.toDelete shouldBe 0L
  }

  // ---- report shape ------------------------------------------------------------------------------

  test("the report counts what the manifest says") {
    val outcome = run(
      Seq(unit("/data/rwa/t/as_of_date=2023-06-30", sizeBytes = 10L),
        unit("/data/rwa/t/as_of_date=2023-03-31", sizeBytes = 20L)),
      policies = Seq(policy(keepMinVersions = 1)))

    outcome.report.candidates shouldBe 2L
    outcome.report.blocked shouldBe 1L
    outcome.report.toDelete shouldBe 1L
    outcome.report.bytesTotal shouldBe 30L
    outcome.report.bytesToFree shouldBe 20L
  }

  test("the manifest keeps the blocked objects, so the report can say what was refused") {
    val outcome = run(Seq(unit(agedOut)), policies = Seq(policy(legalHold = true)))
    outcome.manifest.count() shouldBe 1L
  }

  test("the temporary rule columns do not leak into the manifest") {
    run(Seq(unit(agedOut))).manifest.columns.count(_.startsWith("__ctl_")) shouldBe 0
  }

  test("an empty candidate set reports NOTHING SELECTED rather than CLEAR") {
    run(Seq(unit("/data/rwa/t/as_of_date=2026-06-30"))).report.verdict shouldBe "NOTHING SELECTED"
  }

  test("every control appears in the report, fired or not") {
    run(Seq(unit(agedOut))).report.results.map(_.rule) shouldBe PurgeRule.All
  }
}
