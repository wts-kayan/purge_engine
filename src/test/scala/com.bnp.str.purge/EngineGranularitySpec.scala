package com.bnp.str.purge

import com.bnp.str.purge.engine.{EngineDescriptor, EngineRun, ProjectionEngine, SimulatorClassicEngine}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What a run occupies, and therefore what deleting it removes.
 *
 * `granularity` used to be a field every engine declared and nothing ever read: a table-granular
 * engine would have been expanded with the partition rule, into `<table>/runId=<uuid>` directories
 * it never wrote. Nothing would have failed — the scope would simply have matched nothing, the
 * report would have said zero candidates, and a run that should have removed several tables would
 * have looked like a run with nothing left to do. These tests are what stops that returning: they
 * assert the two granularities produce DIFFERENT scopes, so a reading that ignores the field cannot
 * pass them both.
 */
class EngineGranularitySpec extends AnyFunSuite with Matchers with SparkTestSession {

  /** A stand-in for the simulator (§16.Q1), the engine whose run IS its tables. */
  private object TableGranularEngine extends EngineDescriptor {
    override val name: String = "table-granular-fixture"
    override val partitionKey: String = "runId"
    override val granularity: String = EngineDescriptor.GRANULARITY_TABLE
    override def read(properties: Map[String, String], confPath: String): EngineRun =
      run(granularity)
  }

  private def run(granularity: String): EngineRun =
    EngineRun(
      engine = "fixture", runId = "9df8cf3a", runName = "r", runType = "IFRS9", launchedBy = "u",
      asOfDateQuarter = "2026Q3", database = "dbprojection",
      tables = Seq("term_structure", "lgd"), partitionKey = "runId", granularity = granularity,
      historyTable = "dbprojection.run_history", outputDirectories = Seq.empty,
      inputPaths = Seq.empty, confPath = "conf.properties")

  private val partitionRun = run(EngineDescriptor.GRANULARITY_PARTITION)
  private val tableRun = run(EngineDescriptor.GRANULARITY_TABLE)

  // ---- what a run occupies in one of its tables --------------------------------------------------

  test("a partition-granular run occupies one partition of each table, and leaves the table") {
    partitionRun.relativePathOf("term_structure") shouldBe "term_structure/runId=9df8cf3a"
    partitionRun.partitionSpecOf shouldBe "runid=9df8cf3a"
    partitionRun.isTableGranular shouldBe false
  }

  test("a table-granular run occupies the table itself") {
    tableRun.relativePathOf("term_structure") shouldBe "term_structure"
    tableRun.isTableGranular shouldBe true
  }

  test("a table-granular run claims no partition: it owns every partition of the table, not one") {
    tableRun.partitionSpecOf shouldBe ""
  }

  test("the two granularities never expand to the same path") {
    partitionRun.relativePathOf("lgd") should not be tableRun.relativePathOf("lgd")
  }

  // ---- where a run's identity sits in a path -----------------------------------------------------

  test("PC04's token follows the granularity, since that is where the run id sits on disk") {
    // Partition-granular: the run is a directory inside the table.
    ProjectionEngine.runToken("9df8cf3a") shouldBe "runId=9df8cf3a"
    // Table-granular: there is no such directory, and the table carries the identity.
    TableGranularEngine.runToken("9df8cf3a") shouldBe "9df8cf3a"
  }

  // ---- a granularity nobody implements must not be read as the destructive one -------------------

  test("an unknown granularity is refused when the run is built, not silently treated as PARTITION") {
    val thrown = intercept[IllegalArgumentException](run("PER_FILE"))
    thrown.getMessage should include("PER_FILE")
  }

  test("the descriptor's granularity is validated when it is resolved by name") {
    noException should be thrownBy EngineDescriptor.of("projection")
    EngineDescriptor.of("projection").granularity shouldBe EngineDescriptor.GRANULARITY_PARTITION
  }

  // ---- the engines that declare each granularity ------------------------------------------------

  test("the registry holds one engine of each granularity, and each declares it explicitly") {
    ProjectionEngine.granularity shouldBe EngineDescriptor.GRANULARITY_PARTITION
    SimulatorClassicEngine.granularity shouldBe EngineDescriptor.GRANULARITY_TABLE
    SimulatorClassicEngine.isTableGranular shouldBe true
  }
}
