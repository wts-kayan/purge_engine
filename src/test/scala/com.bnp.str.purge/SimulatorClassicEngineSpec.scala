package com.bnp.str.purge

import com.bnp.str.purge.engine.{EngineDescriptor, SimulatorClassicEngine}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.FileInputStream
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * Reading a real classic-simulator run configuration.
 *
 * As with `ProjectionEngineSpec`, the fixture is the actual file TWIST generated, not a reduced
 * copy — including the keys that are present but empty, the ones whose value is a template rather
 * than a name, and the one key that is declared twice. Those are the parts that decide whether a
 * purge aims at the right tables, so they are the parts worth testing against the real thing.
 *
 * The business named what a purge of this engine removes on 2026-09-07: the four
 * `output.externalTable.name*Output` keys, and nothing else.
 */
class SimulatorClassicEngineSpec extends AnyFunSuite with Matchers {

  private val confPath = "localRun/purge/input/simulator_classic/conf.properties"

  private val properties: Map[String, String] = {
    import scala.collection.JavaConverters._
    val p = new Properties()
    val in = new FileInputStream(confPath)
    try p.load(new java.io.InputStreamReader(in, StandardCharsets.UTF_8)) finally in.close()
    p.asScala.toMap
  }

  private val run = SimulatorClassicEngine.read(properties, confPath)

  private val facility =
    "stapo_sp_starting_point_ifrs9_no_adjust_2026q2_260624_v1_py_10551_pa_input_ste_ifrs926q2_sicr3_v2_20260904_fac"

  // ---- identity ---------------------------------------------------------------------------------

  test("the run is identified by what the simulator recorded about itself") {
    run.engine shouldBe "simulator_classic"
    run.runId shouldBe "5bbad7c8-8e7d-4e88-8106-f00ff4345e0b"
    run.launchedBy shouldBe "539693"
    run.asOfDateQuarter shouldBe "2026Q2"
    run.database shouldBe "dbsimulateur"
  }

  test("the run is table-granular: it IS its tables, not a partition of them") {
    run.granularity shouldBe EngineDescriptor.GRANULARITY_TABLE
    run.isTableGranular shouldBe true
    run.relativePathOf(facility) shouldBe facility
    run.partitionSpecOf shouldBe ""
  }

  // ---- what a purge of this run removes ----------------------------------------------------------

  private val byTerm =
    "stapo_sp_starting_point_ifrs9_no_adjust_2026q2_260624_v1_py_10551_pa_input_ste_ifrs926q2_sicr3_v2_20260904_fac_measure_bytermoutput"

  test("the two tables the four output keys resolve to, each with its non-sectoral twin") {
    run.tables should contain theSameElementsAs Seq(
      facility, facility + "_nosecto",
      byTerm, byTerm + "_nosecto")
  }

  // ---- the _nosecto twins ------------------------------------------------------------------------
  //
  // output.suffix.for_non_secto_table = nosecto, and every output may exist twice: once for the full
  // perimeter and once for the non-sectoral one. The four keys name only the first of each pair, so
  // a purge built from them alone leaves the twin behind — a run half-removed.

  test("each declared table is scoped with its non-sectoral twin") {
    run.tables should contain(facility + "_nosecto")
    run.tables should contain(byTerm + "_nosecto")
  }

  test("the suffix is read from the configuration, not assumed") {
    val other = SimulatorClassicEngine.read(
      properties + ("output.suffix.for_non_secto_table" -> "nosec"), confPath)
    other.tables should contain(facility + "_nosec")
    other.tables should not contain (facility + "_nosecto")
  }

  test("no suffix declared means no twin invented") {
    val none = SimulatorClassicEngine.read(
      properties + ("output.suffix.for_non_secto_table" -> ""), confPath)
    none.tables should have size 2
    none.tables.exists(_.contains("nosecto")) shouldBe false
  }

  test("a table already carrying the suffix is not suffixed twice") {
    val already = SimulatorClassicEngine.read(
      properties + ("output.externalTable.nameFacilityOutput" -> "dbsimulateur.some_table_nosecto")
        - "output.externalTable.nameFacilityWeightedOutput", confPath)
    already.tables should contain("some_table_nosecto")
    already.tables should not contain "some_table_nosecto_nosecto"
  }

  test("a twin is written the same way Hive holds it: lower case") {
    run.tables.filter(_.endsWith("_nosecto")).foreach(t => t shouldBe t.toLowerCase)
  }

  test("the same table named by two keys is listed once") {
    // nameFacilityOutput and nameFacilityWeightedOutput hold one and the same value; counting it
    // twice would purge it twice and double the bytes purge_detail reports freed.
    run.tables.count(_ == facility) shouldBe 1
  }

  test("an output that was switched off names no table") {
    // output.externalTable.nameFacilityMeasurementOutput= is empty in the fixture.
    run.tables.exists(_.endsWith("_fac_measurementoutput")) shouldBe false
    // two declared tables, each with its non-sectoral twin
    run.tables should have size 4
  }

  test("the database qualifier is split off and the name lower-cased for Hive") {
    run.tables.foreach { table =>
      table should not startWith "dbsimulateur."
      table shouldBe table.toLowerCase
    }
  }

  // ---- and what it must NOT remove ---------------------------------------------------------------

  test("the shared results table and the run metadata are not the run's to delete") {
    run.tables should not contain "simulation_results_fac_partitioned_full"
    run.tables should not contain "run_metadata"
  }

  test("no template is ever read as a table name") {
    // output.externalTable.name = output_%s_%t, and the three monte-carlo keys are templates too.
    run.tables.exists(_.contains("%")) shouldBe false
    run.tables.exists(_.startsWith("output_")) shouldBe false
    run.tables.exists(_.contains("quantiles")) shouldBe false
  }

  test("no output DIRECTORY is collected — least of all the root of every simulator run") {
    // output.path.run.directory is /Projects/.../Output_Simulateur/, which holds every run this
    // engine has ever written. Collected as this run's output it would put that whole tree in a
    // purge scope, deep enough and unlisted enough that PurgeGuard would not save it.
    run.outputDirectories shouldBe empty
  }

  // ---- the inputs PC14 protects ------------------------------------------------------------------

  test("inputs declared outside input.path.* are still collected") {
    // A prefix rule copied from projection would have missed these five shared referentials.
    val lgd = run.inputPaths.filter(_.contains("Output_Cluster/IHM/10551"))
    lgd.exists(_.endsWith("lgd_term_structure_simulator_format_quarterly.csv")) shouldBe true
    lgd.exists(_.endsWith("MappingTsLgd.csv")) shouldBe true
  }

  test("the portfolio, the referentials and the parameter set are all protected") {
    run.inputPaths.exists(_.contains("/portfolio/")) shouldBe true
    run.inputPaths.exists(_.contains("/ReferentielExterne/")) shouldBe true
    run.inputPaths.exists(_.contains("/ReferentielInterne/")) shouldBe true
    run.inputPaths.exists(_.contains("/Simulateur/Parameter/")) shouldBe true
  }

  test("another engine's output, which this run consumed, is protected too") {
    // The simulation reads projection 10551's output. Purging THIS run must never remove it.
    run.inputPaths.exists(_.contains("/Projection_Production/Output_Cluster/IHM/10551/")) shouldBe true
  }

  test("every protected path is an absolute path, not a flag that happens to start with input") {
    run.inputPaths.foreach(_ should startWith("/"))
    // input.lgd_forward_looking.epsilon_for_hlc = 0.0001 is a number, not a path.
    run.inputPaths.exists(_.contains("0.0001")) shouldBe false
  }

  // ---- the engine's own history ------------------------------------------------------------------

  test("the unqualified history table is qualified with the run's own database") {
    // run.history.tablename = RUN_HISTORY, with no database, where projection declares it in full.
    run.historyTable shouldBe "dbsimulateur.run_history"
  }

  // ---- refusals ----------------------------------------------------------------------------------

  test("a configuration naming no output table is refused, not read as an empty purge") {
    val thrown = intercept[IllegalArgumentException](
      SimulatorClassicEngine.read(properties - "output.externalTable.nameFacilityOutput"
        - "output.externalTable.nameFacilityWeightedOutput"
        - "output.externalTable.nameFacilityMeasurementByTermOutput", confPath))
    thrown.getMessage should include("nothing to remove")
  }

  test("a table in another database is refused rather than purged across databases") {
    val elsewhere = properties +
      ("output.externalTable.nameFacilityOutput" -> "dbprojection.term_structure")
    intercept[IllegalArgumentException](SimulatorClassicEngine.read(elsewhere, confPath))
      .getMessage should include("will not cross databases")
  }

  test("the engine is resolvable by the name an operator writes in the configuration") {
    EngineDescriptor.of("simulator_classic") shouldBe SimulatorClassicEngine
    EngineDescriptor.of("SIMULATOR_CLASSIC").granularity shouldBe EngineDescriptor.GRANULARITY_TABLE
  }
}
