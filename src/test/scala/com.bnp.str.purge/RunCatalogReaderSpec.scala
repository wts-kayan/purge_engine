package com.bnp.str.purge

import com.bnp.str.purge.reader.RunCatalogReader
import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

/**
 * The listing the IHM's scope screen is built on.
 *
 * The property under test throughout: the catalogue describes WHAT IS ON DISK, because that is what
 * a purge would remove. The engine's history annotates it and never filters it.
 */
class RunCatalogReaderSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll with SparkTestSession {

  private var tmp: Path = _
  private var dbLocation: String = _

  private val runA = "9df8cf3a-c2fa-4bfd-9068-aa6587ba84cf"
  private val runB = "5c1f53c8-6afe-41d6-9a5e-9953219cde05"

  override def beforeAll(): Unit = {
    tmp = Files.createTempDirectory("run-catalog-spec")
    val db = tmp.resolve("projection").resolve("dbprojection.db")

    // run A: three tables. run B: one. Plus a table directory and a history table that are not runs.
    Seq("term_structure", "projected_dr", "lgd").foreach(partitionOf(db, _, runA, "a" * 100))
    partitionOf(db, "term_structure", runB, "b" * 50)
    Files.createDirectories(db.resolve("run_history"))
    Files.write(db.resolve("run_history").resolve("part-00000.orc"), "history".getBytes(StandardCharsets.UTF_8))

    dbLocation = PrimaryUtilities.normalizePath(db.toString)
  }

  override def afterAll(): Unit = if (tmp != null) FileUtils.deleteQuietly(tmp.toFile)

  private def partitionOf(db: Path, table: String, runId: String, content: String): Unit = {
    val dir = db.resolve(table).resolve(s"runId=$runId")
    Files.createDirectories(dir)
    Files.write(dir.resolve("part-00000.orc"), content.getBytes(StandardCharsets.UTF_8))
  }

  private def conf(engineExtra: String = ""): Config =
    ConfigFactory.parseString(
      s"""${PrimaryConstants.APP_CONF} {
         |  ${PrimaryConstants.ENGINE} {
         |    name = "projection"
         |    databaseLocation = "$dbLocation"
         |    $engineExtra
         |  }
         |  scan {
         |    allowedRoots = [ "$dbLocation" ]
         |    roots        = [ "$dbLocation" ]
         |    databases    = []
         |    maxDepth     = 4
         |    parallelism  = 2
         |  }
         |}""".stripMargin)

  private def catalog(config: Config = conf()): Map[String, Row] =
    new RunCatalogReader()(spark, config).read().collect().map(r => r.getAs[String]("run_id") -> r).toMap

  // ---- what it finds ------------------------------------------------------------------------------

  test("every run on disk is listed, once") {
    val runs = catalog()
    runs.keySet shouldBe Set(runA, runB)
  }

  test("a run is measured across every table it wrote") {
    val a = catalog()(runA)
    a.getAs[Long]("table_count") shouldBe 3L
    a.getAs[Long]("object_count") shouldBe 3L
    a.getAs[Long]("size_bytes") shouldBe 300L
  }

  test("runs are measured independently of each other") {
    val b = catalog()(runB)
    b.getAs[Long]("table_count") shouldBe 1L
    b.getAs[Long]("size_bytes") shouldBe 50L
  }

  test("the engine is named on every row, so the IHM can list several at once") {
    catalog().values.map(_.getAs[String]("engine")).toSet shouldBe Set("projection")
  }

  test("a run carries when it was written, for the screen to sort on") {
    val a = catalog()(runA)
    a.getAs[java.sql.Timestamp]("first_written") should not be null
    a.getAs[java.sql.Timestamp]("last_written") should not be null
  }

  // ---- what it leaves out --------------------------------------------------------------------------

  test("a directory carrying no run id is not a run") {
    // the engine's history table lives beside the output tables and is emphatically not a run
    catalog().keySet should not contain "run_history"
    catalog().size shouldBe 2
  }

  // ---- what the history adds, and does not remove ---------------------------------------------------

  test("with no history, every run is still listed and reported as unknown to it") {
    // a catalogue that hid what the history cannot confirm would hide exactly the runs worth purging
    val runs = catalog()
    runs(runA).getAs[Boolean]("known_to_history") shouldBe false
    runs(runB).getAs[Boolean]("known_to_history") shouldBe false
  }

  test("an unreadable history table does not fail the catalogue") {
    // the catalogue has no run configurations to derive a history table from, and must not need any
    val runs = catalog(conf("""historyTable = "no_such_db.run_history"""" ))
    runs.keySet shouldBe Set(runA, runB)
    runs(runA).getAs[Boolean]("known_to_history") shouldBe false
  }

  test("the catalogue needs no run configuration: it lists runs rather than acting on one") {
    // asking a listing for the configuration of the run to list would be asking for the answer
    // before the question
    new com.bnp.str.purge.reader.EngineRunReader()(spark, conf()).historyTables shouldBe empty
    catalog().keySet shouldBe Set(runA, runB)
  }

  // ---- the shape the IHM reads ------------------------------------------------------------------------

  test("the catalogue carries every column the scope screen shows") {
    val columns = new RunCatalogReader()(spark, conf()).read().columns.toSet
    RunCatalogReader.COLUMNS.foreach(columns should contain(_))
  }
}
