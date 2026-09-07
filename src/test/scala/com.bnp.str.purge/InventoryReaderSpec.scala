package com.bnp.str.purge

import com.bnp.str.purge.reader.InventoryReader
import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

/**
 * The inventory is walked against a real temporary tree rather than a mock filesystem: the whole
 * point of these tests is that the walk agrees with what a `FileSystem` actually reports —
 * a mocked listing would agree with itself and prove nothing.
 *
 * Tree built for the suite (`<tmp>/datalake/rwa` is the scan root):
 *
 *   datalake/rwa/ts_ead_fwd/_SUCCESS                     <- FILE   (sits beside sub-directories)
 *   datalake/rwa/ts_ead_fwd/as_of_date=2023-03-31/    <- LEAF_DIR, 2 orc files
 *   datalake/rwa/ts_ead_fwd/as_of_date=2023-06-30/    <- LEAF_DIR, 1 orc file
 *   datalake/rwa/crr_param/as_of_date=2024-12-31/    <- LEAF_DIR, 1 csv file
 *   datalake/rwa/empty_table/                            <- LEAF_DIR, 0 files
 */
class InventoryReaderSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll with SparkTestSession {

  private var tmpRoot: Path = _
  private var scanRoot: String = _
  private var allowedRoot: String = _

  override def beforeAll(): Unit = {
    tmpRoot = Files.createTempDirectory("purge-inventory-spec")
    val rwa = tmpRoot.resolve("datalake").resolve("rwa")

    writeFile(rwa.resolve("ts_ead_fwd").resolve("_SUCCESS"), "")
    writeFile(rwa.resolve("ts_ead_fwd").resolve("as_of_date=2023-03-31").resolve("part-00000.orc"), "a" * 100)
    writeFile(rwa.resolve("ts_ead_fwd").resolve("as_of_date=2023-03-31").resolve("part-00001.orc"), "b" * 200)
    writeFile(rwa.resolve("ts_ead_fwd").resolve("as_of_date=2023-06-30").resolve("part-00000.orc"), "c" * 50)
    writeFile(rwa.resolve("crr_param").resolve("as_of_date=2024-12-31").resolve("part-00000.csv"), "d" * 10)
    Files.createDirectories(rwa.resolve("empty_table"))

    scanRoot = PrimaryUtilities.normalizePath(rwa.toString)
    allowedRoot = PrimaryUtilities.normalizePath(tmpRoot.resolve("datalake").toString)
  }

  override def afterAll(): Unit =
    if (tmpRoot != null) FileUtils.deleteQuietly(tmpRoot.toFile)

  private def writeFile(path: Path, content: String): Unit = {
    Files.createDirectories(path.getParent)
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }

  /** A conf holding only the `scan` block the reader needs, with the knobs a test wants to vary. */
  private def confFor(roots: Seq[String],
                      allowedRoots: Seq[String],
                      maxDepth: Int = 6,
                      parallelism: Int = 4): Config = {
    def list(values: Seq[String]) = values.map(v => "\"" + v + "\"").mkString("[", ", ", "]")
    ConfigFactory.parseString(
      s"""${PrimaryConstants.APP_CONF} {
         |  scan {
         |    allowedRoots = ${list(allowedRoots)}
         |    roots        = ${list(roots)}
         |    databases    = []
         |    maxDepth     = $maxDepth
         |    parallelism  = $parallelism
         |    followSymlinks = false
         |  }
         |}""".stripMargin)
  }

  private def inventory(config: Config): Seq[InventoryReader.PurgeUnit] = {
    import spark.implicits._
    new InventoryReader()(spark, config).read().as[InventoryReader.PurgeUnit].collect().toSeq
  }

  // ---- what a purge unit is --------------------------------------------------------------------

  test("a directory with no sub-directory is one LEAF_DIR unit carrying the aggregate of its files") {
    val units = inventory(confFor(Seq(scanRoot), Seq(allowedRoot)))
    val partition = units.find(_.path.endsWith("as_of_date=2023-03-31")).getOrElse(
      fail(s"partition not inventoried; got: ${units.map(_.path).mkString(", ")}"))

    partition.object_type shouldBe PrimaryConstants.OBJECT_TYPE_LEAF_DIR
    partition.num_files shouldBe 2L
    partition.size_bytes shouldBe 300L
  }

  test("a file sitting beside sub-directories is a unit of its own") {
    val units = inventory(confFor(Seq(scanRoot), Seq(allowedRoot)))
    val success = units.find(_.path.endsWith("ts_ead_fwd/_SUCCESS")).getOrElse(
      fail("the file beside the partition folders was not inventoried"))

    success.object_type shouldBe PrimaryConstants.OBJECT_TYPE_FILE
    success.num_files shouldBe 1L
  }

  test("an empty directory is reported, not skipped") {
    // it is exactly what a previous purge leaves behind, so the report has to show it
    val units = inventory(confFor(Seq(scanRoot), Seq(allowedRoot)))
    val empty = units.find(_.path.endsWith("empty_table")).getOrElse(fail("the empty directory was dropped"))

    empty.object_type shouldBe PrimaryConstants.OBJECT_TYPE_LEAF_DIR
    empty.num_files shouldBe 0L
    empty.size_bytes shouldBe 0L
  }

  test("a directory holding sub-directories is not itself a unit") {
    val units = inventory(confFor(Seq(scanRoot), Seq(allowedRoot)))
    units.map(_.path).find(_.endsWith("/ts_ead_fwd")) shouldBe None
  }

  test("every unit of the scope is found exactly once, whatever the seeding did") {
    val units = inventory(confFor(Seq(scanRoot), Seq(allowedRoot)))
    units.map(_.path).distinct.size shouldBe units.size
    units.size shouldBe 5 // 3 partition folders + 1 empty folder + 1 loose file
    units.map(_.size_bytes).sum shouldBe 360L
    units.foreach(_.root_path shouldBe scanRoot)
  }

  test("the walk gives the same answer whether it runs on the driver or on the executors") {
    // parallelism 1 keeps everything on the driver seeding loop; 16 pushes it to the executors
    val onDriver = inventory(confFor(Seq(scanRoot), Seq(allowedRoot), parallelism = 1)).map(_.path).sorted
    val onExecutors = inventory(confFor(Seq(scanRoot), Seq(allowedRoot), parallelism = 16)).map(_.path).sorted
    onExecutors shouldBe onDriver
  }

  // ---- maxDepth ---------------------------------------------------------------------------------

  test("maxDepth stops the descent and reports the sub-tree from the NameNode summary") {
    val units = inventory(confFor(Seq(scanRoot), Seq(allowedRoot), maxDepth = 1))
    val truncated = units.filter(_.object_type == PrimaryConstants.OBJECT_TYPE_DIR_TRUNCATED)

    truncated.map(u => PrimaryUtilities.baseName(u.path)).toSet shouldBe Set("ts_ead_fwd", "crr_param")
    // a scope is never reported as smaller than it is just because it is deeper than expected
    units.map(_.size_bytes).sum shouldBe 360L
  }

  // ---- root validation --------------------------------------------------------------------------

  test("a root outside the allowed roots is refused before the first listing") {
    val outside = PrimaryUtilities.normalizePath(tmpRoot.resolve("elsewhere").resolve("a").resolve("b").resolve("c").toString)
    val thrown = intercept[IllegalArgumentException](inventory(confFor(Seq(outside), Seq(allowedRoot))))
    thrown.getMessage should include("outside the allowed roots")
  }

  test("a forbidden root is refused whatever the allowed roots say") {
    val thrown = intercept[IllegalArgumentException](inventory(confFor(Seq("/tmp"), Seq("/tmp"))))
    thrown.getMessage should (include("forbidden") or include("depth"))
  }

  test("a root shallower than the minimum depth is refused") {
    intercept[IllegalArgumentException](inventory(confFor(Seq("/data/x"), Seq("/data"))))
  }

  test("every invalid root is named at once, so the conf is fixed in one pass") {
    val bad1 = "/data/promethee/str/other"
    val bad2 = "/data/promethee/str/another"
    val thrown = intercept[IllegalArgumentException](
      inventory(confFor(Seq(bad1, bad2), Seq("/data/promethee/str/rwa"))))
    thrown.getMessage should include(bad1)
    thrown.getMessage should include(bad2)
  }

  test("an empty roots list is refused rather than silently inventorying nothing") {
    intercept[IllegalArgumentException](inventory(confFor(Seq.empty, Seq(allowedRoot))))
  }

  test("a root that does not exist is skipped with a warning, not a failure") {
    val missing = PrimaryUtilities.normalizePath(tmpRoot.resolve("datalake").resolve("ifrs9").toString)
    val units = inventory(confFor(Seq(scanRoot, missing), Seq(allowedRoot)))
    units.size shouldBe 5
  }

  // ---- a root pointing straight at a file --------------------------------------------------------

  test("a root that is a file is inventoried as a FILE unit") {
    val filePath = PrimaryUtilities.normalizePath(
      Paths.get(tmpRoot.toString, "datalake", "rwa", "ts_ead_fwd", "_SUCCESS").toString)
    val units = inventory(confFor(Seq(filePath), Seq(allowedRoot)))

    units.size shouldBe 1
    units.head.object_type shouldBe PrimaryConstants.OBJECT_TYPE_FILE
  }
}
