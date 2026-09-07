package com.bnp.str.purge

import com.bnp.str.purge.reader.EngineRunReader
import com.bnp.str.purge.utility.PrimaryConstants
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Expanding an engine run into the objects a purge acts on.
 *
 * The run is the unit an operator picks in TWIST; these tests hold the expansion to what that
 * implies — every output table of the run, on both sides of the metastore/filesystem divide, and
 * nothing the run merely read.
 */
class EngineRunReaderSpec extends AnyFunSuite with Matchers with SparkTestSession {

  private val confPath = "localRun/purge/input/projection/conf.properties"
  private val dbLocation = "localRun/purge/input/projection/dbprojection.db"
  private val runId = "9df8cf3a-c2fa-4bfd-9068-aa6587ba84cf"

  private def conf(engine: String): Config =
    ConfigFactory.parseString(
      s"""${PrimaryConstants.APP_CONF} {
         |  scan { allowedRoots = [], roots = [], databases = [] }
         |  ${PrimaryConstants.ENGINE} { $engine }
         |}""".stripMargin)

  private val projection =
    s"""name = "projection"
       |runConfPaths = [ "$confPath" ]
       |databaseLocation = "$dbLocation"""".stripMargin

  private def reader(engine: String = projection) = new EngineRunReader()(spark, conf(engine))

  // ---- is this run engine-driven at all? ---------------------------------------------------------

  test("no engine block means a policy- or path-driven purge, not an error") {
    val config = ConfigFactory.parseString(
      s"""${PrimaryConstants.APP_CONF} { scan { allowedRoots = [], roots = [], databases = [] } }""")
    val engineReader = new EngineRunReader()(spark, config)

    engineReader.isConfigured shouldBe false
    engineReader.runs shouldBe empty
    engineReader.scopeEntries shouldBe empty
    engineReader.protectedPaths shouldBe empty
  }

  // ---- the expansion -----------------------------------------------------------------------------

  test("a run expands to every one of its output tables, on both sides of the metastore") {
    val entries = reader().scopeEntries
    val run = reader().runs.head

    // one metastore identity and one path per table, plus the run's output folder
    val byIdentity = entries.filter(_.scope_path.isEmpty)
    val byPath = entries.filter(_.scope_path.nonEmpty)

    byIdentity.size shouldBe run.tables.size
    byIdentity.map(_.scope_database).distinct shouldBe Seq("dbprojection")
    byPath.size shouldBe run.tables.size + run.outputDirectories.size
  }

  test("the metastore identity uses the lowercase partition key Hive stores") {
    reader().scopeEntries
      .filter(_.scope_path.isEmpty)
      .map(_.scope_partition_spec)
      .distinct shouldBe Seq(s"runid=$runId")
  }

  test("the path uses the capital-I spelling the engine actually writes") {
    val paths = reader().scopeEntries.map(_.scope_path).filter(_.nonEmpty)

    paths.exists(_.endsWith(s"/term_structure/runId=$runId")) shouldBe true
    paths.exists(_.contains(s"runid=$runId")) shouldBe false
  }

  test("the run's non-Hive output folder is in scope too") {
    reader().scopeEntries.map(_.scope_path).exists(_.contains("Output_Cluster/IHM/10463")) shouldBe true
  }

  test("every scope entry carries the run's as-of date as the age to judge it by") {
    // 2026Q3 -> the last day of that quarter; the files say when the run was EXECUTED, which for a
    // re-run of an old vintage is the wrong age entirely
    reader().scopeEntries.map(_.scope_business_date).distinct shouldBe Seq("2026-09-30")
  }

  test("without a resolvable database location the run is still scoped by metastore identity") {
    val entries = reader(s"""name = "projection", runConfPaths = [ "$confPath" ]""").scopeEntries

    entries should not be empty
    entries.filter(_.scope_path.nonEmpty).map(_.scope_path)
      .filterNot(_.contains("Output_Cluster")) shouldBe empty
    entries.filter(_.scope_path.isEmpty) should not be empty
  }

  // ---- what must never be purged -----------------------------------------------------------------

  test("the shared inputs the run declares are protected") {
    val protectedPaths = reader().protectedPaths

    protectedPaths.exists(_.endsWith("scen_IFRS9_2026Q3draft_mef.csv")) shouldBe true
    protectedPaths.exists(_.contains("modelsTemplate_ifrs9_26Q2_v0.csv")) shouldBe true
  }

  test("the engine's own run history is protected") {
    // it is what proves the purge was authorised
    reader().protectedPaths.exists(_.endsWith("/run_history")) shouldBe true
  }

  test("no protected path is also a scope entry") {
    val scoped = reader().scopeEntries.map(_.scope_path).filter(_.nonEmpty).toSet
    reader().protectedPaths.foreach(p => scoped should not contain p)
  }

  // ---- refusals ------------------------------------------------------------------------------------

  test("an unknown engine name is refused") {
    intercept[IllegalArgumentException](reader("""name = "tseadfwd", runConfPaths = [ "x" ]""").runs)
      .getMessage should include("Unknown engine")
  }

  test("naming an engine without naming a run is refused") {
    intercept[IllegalArgumentException](reader("""name = "projection"""").runs)
      .getMessage should include("runConfPaths")
  }

  test("the same run named twice is refused rather than counted twice") {
    val thrown = intercept[IllegalArgumentException](
      reader(s"""name = "projection"
                |runConfPaths = [ "$confPath" ]
                |runConfPath = "./$confPath"""".stripMargin).runs)
    thrown.getMessage should include(runId)
  }

  test("a missing run configuration fails the run rather than purging an empty scope") {
    // silently scoping nothing is indistinguishable from a purge with nothing to do
    intercept[Exception](reader(s"""name = "projection", runConfPaths = [ "no/such/conf.properties" ]""").runs)
  }
}
