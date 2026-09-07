package com.bnp.str.purge

import com.bnp.str.purge.engine.{EngineDescriptor, ProjectionEngine}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.FileInputStream
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * Reading a real projection run configuration.
 *
 * The fixture is the actual `conf.properties` a projection run was launched with, not a reduced
 * copy: what this engine has to survive is the file as the projection team writes it, including the
 * keys that are present but empty and the ones whose spelling does not match what lands on disk.
 */
class ProjectionEngineSpec extends AnyFunSuite with Matchers {

  private val confPath = "localRun/purge/input/projection/conf.properties"

  private val properties: Map[String, String] = {
    import scala.collection.JavaConverters._
    val p = new Properties()
    val in = new FileInputStream(confPath)
    try p.load(new java.io.InputStreamReader(in, StandardCharsets.UTF_8)) finally in.close()
    p.asScala.toMap
  }

  private val run = ProjectionEngine.read(properties, confPath)

  // ---- identity ---------------------------------------------------------------------------------

  test("the run is identified by what the engine recorded about itself") {
    run.engine shouldBe "projection"
    run.runId shouldBe "9df8cf3a-c2fa-4bfd-9068-aa6587ba84cf"
    run.runName shouldBe "IFRS9_scn26Q3_scenDraft"
    run.runType shouldBe "IFRS9"
    run.launchedBy shouldBe "539693"
    run.asOfDateQuarter shouldBe "2026Q3"
    run.database shouldBe "dbprojection"
  }

  test("a projection run is one partition per table, not a whole table") {
    // the simulator is the exception (Q1); getting this wrong drops tables where a partition was meant
    run.granularity shouldBe EngineDescriptor.GRANULARITY_PARTITION
  }

  test("the partition is spelled one way on disk and another in the metastore") {
    // observed under dbprojection.db/term_structure: capital I. Hive lowercases keys in the
    // catalogue, so both spellings are needed and neither side should have to guess.
    run.partitionDirectory shouldBe "runId=9df8cf3a-c2fa-4bfd-9068-aa6587ba84cf"
    run.partitionSpec shouldBe "runid=9df8cf3a-c2fa-4bfd-9068-aa6587ba84cf"
  }

  // ---- outputs ----------------------------------------------------------------------------------

  test("every output table the run declares is collected") {
    run.tables should contain allOf("term_structure", "term_structure_detailed", "projected_dr",
      "projected_z", "migration_matrix", "lgd", "pcure", "chr", "scenarii_ponderation",
      "run_projection", "migration_matrix_npl")
    run.tables.size should be > 20
  }

  test("table names are lowercased to match what Hive puts on disk") {
    // the conf spells `projected_CR_detailed`; HDFS holds `projected_cr_detailed`, and a path built
    // from the conf spelling simply would not exist — a purge that finds nothing looks exactly like
    // a purge with nothing to do
    properties("output.table.name.projectedDR") shouldBe "projected_DR"
    run.tables should contain("projected_dr")
    run.tables should not contain "projected_DR"
    run.tables.foreach(table => table shouldBe table.toLowerCase)
  }

  test("the same table named by two keys is counted once") {
    run.tables.distinct.size shouldBe run.tables.size
  }

  test("the engine's run history is not one of the outputs") {
    // it is the audit of every run ever made, and the row describing this run is the evidence that
    // the purge was legitimate
    run.historyTable shouldBe "dbprojection.run_history"
    run.tables should not contain "run_history"
  }

  test("the non-Hive output folder of the run is collected") {
    run.outputDirectories shouldBe
      Seq("/Projects/STCreditRisk_STE/Projection_Production/Output_Cluster/IHM/10463/")
  }

  // ---- inputs -----------------------------------------------------------------------------------

  test("every declared input is collected, so PC14 can refuse to delete one") {
    run.inputPaths should contain(
      "/Projects/STCreditRisk_STE/STRESSTEST/Projection/Scenario/scen_IFRS9_2026Q3draft_mef.csv")
    run.inputPaths.size should be > 15
  }

  test("an input key that is present but empty is not a path") {
    properties("input.path.rho") shouldBe ""
    run.inputPaths.foreach(_ should not be empty)
  }

  test("no output path is mistaken for an input") {
    run.inputPaths.foreach(input => run.outputDirectories should not contain input)
  }

  // ---- refusals ---------------------------------------------------------------------------------

  test("a configuration with no run id is refused") {
    val thrown = intercept[IllegalArgumentException](
      ProjectionEngine.read(properties - "projection.run.id", confPath))
    thrown.getMessage should include("projection.run.id")
  }

  test("a configuration with no output database is refused") {
    intercept[IllegalArgumentException](
      ProjectionEngine.read(properties - "output.database.name", confPath))
      .getMessage should include("output.database.name")
  }

  // ---- the registry ------------------------------------------------------------------------------

  test("the engine is resolved by name, case-insensitively") {
    EngineDescriptor.of("projection") shouldBe ProjectionEngine
    EngineDescriptor.of("PROJECTION") shouldBe ProjectionEngine
  }

  test("an unknown engine is refused, and the message says what is supported") {
    val thrown = intercept[IllegalArgumentException](EngineDescriptor.of("ageing"))
    thrown.getMessage should include("ageing")
    thrown.getMessage should include("projection")
  }
}
