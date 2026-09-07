package com.bnp.str.purge

import com.bnp.str.purge.purge.{PurgeExecutor, PurgeResult}
import com.bnp.str.purge.utility.PrimaryConstants
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.sql.Timestamp

/**
 * The only class in the engine that deletes, tested against a real filesystem.
 *
 * A mocked filesystem would agree with itself and prove nothing about the one property that matters
 * here: that after this code runs, the right bytes are gone and the wrong ones are still there. So
 * every test builds a real tree, purges it, and then looks.
 */
class PurgeExecutorSpec extends AnyFunSuite with Matchers with BeforeAndAfterEach with SparkTestSession {

  private var tmp: Path = _

  /** Trash needs a non-zero interval to do anything; the cluster's is 7 days (Q5). */
  private val TRASH_MINUTES = 7 * 24 * 60

  override def beforeEach(): Unit = {
    tmp = Files.createTempDirectory("purge-executor-spec")
    spark.sparkContext.hadoopConfiguration.setLong("fs.trash.interval", TRASH_MINUTES.toLong)
  }

  override def afterEach(): Unit = {
    if (tmp != null) FileUtils.deleteQuietly(tmp.toFile)
    spark.sparkContext.hadoopConfiguration.setLong("fs.trash.interval", 0L)
  }

  // ---- fixtures ----------------------------------------------------------------------------------

  private def partition(name: String, content: String = "some data"): Path = {
    val dir = tmp.resolve(name)
    Files.createDirectories(dir)
    Files.write(dir.resolve("part-00000.orc"), content.getBytes(StandardCharsets.UTF_8))
    dir
  }

  private def conf(execution: String = ""): Config =
    ConfigFactory.parseString(
      s"""${PrimaryConstants.APP_CONF} {
         |  scan { allowedRoots = [], roots = [], databases = [] }
         |  ${PrimaryConstants.EXECUTION} { $execution }
         |}""".stripMargin)

  /** One manifest row, matching the object on disk unless a test deliberately makes it disagree. */
  private def row(path: Path,
                  decision: String = PrimaryConstants.DECISION_DELETE,
                  strategy: String = PrimaryConstants.STRATEGY_TRASH,
                  sizeBytes: Long = -1L,
                  modificationTime: Timestamp = null): ManifestRow = {
    ManifestRow(
      path = com.bnp.str.purge.utility.PrimaryUtilities.normalizePath(path.toString),
      request_id = "PRG-1",
      database_name = "",
      table_name = "",
      partition_spec = "",
      is_registered_partition = false,
      strategy = strategy,
      decision = decision,
      // as the inventory reports it: a LEAF_DIR's size is the sum of the files it holds, never the
      // directory's own length (which is 0 on Windows and a block size elsewhere)
      size_bytes = if (sizeBytes >= 0L) sizeBytes else contentSize(path),
      num_files = 1L,
      // likewise the mtime: the NEWEST of the directory and the files it holds
      modification_time =
        if (modificationTime != null) modificationTime else new Timestamp(contentMtime(path)))
  }

  private def manifestOf(rows: ManifestRow*): DataFrame = {
    import spark.implicits._
    spark.createDataset(rows.toList).toDF()
  }

  private def purgeRun(rows: Seq[ManifestRow], config: Config = conf()): PurgeResult =
    new PurgeExecutor()(spark, config).execute(manifestOf(rows: _*), "run-1", "j03627")

  private def exists(p: Path): Boolean = Files.exists(p)

  /** Total bytes of the files directly under `path`, or the file's own length. */
  private def contentSize(path: Path): Long =
    if (!Files.isDirectory(path)) path.toFile.length()
    else Option(path.toFile.listFiles()).map(_.filter(_.isFile).map(_.length()).sum).getOrElse(0L)

  /** Newest of the directory's own timestamp and those of the files it holds. */
  private def contentMtime(path: Path): Long =
    if (!Files.isDirectory(path)) path.toFile.lastModified()
    else (path.toFile.lastModified() +:
      Option(path.toFile.listFiles()).map(_.filter(_.isFile).map(_.lastModified()).toSeq).getOrElse(Seq.empty)).max

  // ---- the happy path ----------------------------------------------------------------------------

  test("a TRASH deletion removes the data and says where it went") {
    val dir = partition("term_structure/runId=abc")
    val result = purgeRun(Seq(row(dir)))

    exists(dir) shouldBe false
    result.records.head.status shouldBe PurgeExecutor.STATUS_TRASHED
    result.records.head.trashPath should include(".Trash/Current")
    result.status shouldBe "SUCCESS"
  }

  test("a TRASH deletion records a restore deadline from the cluster's trash interval") {
    val dir = partition("t/runId=abc")
    val record = purgeRun(Seq(row(dir))).records.head

    record.restoreDeadline should not be empty
    val window = record.restoreDeadline.get.getTime - record.executedAt.getTime
    window shouldBe (TRASH_MINUTES.toLong * 60L * 1000L)
  }

  test("a HARD deletion removes the data and promises no way back") {
    val dir = partition("t/runId=abc")
    val record = purgeRun(Seq(row(dir, strategy = PrimaryConstants.STRATEGY_HARD))).records.head

    exists(dir) shouldBe false
    record.status shouldBe PurgeExecutor.STATUS_DELETED
    record.trashPath shouldBe ""
    record.restoreDeadline shouldBe None
  }

  test("a LOGICAL deletion records the object as purged and leaves it alone") {
    val dir = partition("t/runId=abc")
    val record = purgeRun(Seq(row(dir, strategy = PrimaryConstants.STRATEGY_LOGICAL))).records.head

    exists(dir) shouldBe true
    record.status shouldBe PurgeExecutor.STATUS_LOGICALLY_DELETED
    record.bytesFreed shouldBe 0L
  }

  test("the bytes reported freed are the bytes the manifest accounted for") {
    val dir = partition("t/runId=abc", content = "x" * 500)
    purgeRun(Seq(row(dir))).bytesFreed shouldBe 500L
  }

  // ---- what it refuses to touch --------------------------------------------------------------------

  test("a BLOCKED row is recorded, not deleted") {
    // purge_detail is a complete account of the manifest, not a list of successes
    val dir = partition("t/runId=abc")
    val result = purgeRun(Seq(row(dir, decision = PrimaryConstants.DECISION_BLOCKED)))

    exists(dir) shouldBe true
    result.records.head.status shouldBe PurgeExecutor.STATUS_SKIPPED_BLOCKED
    result.removed shouldBe empty
  }

  test("a CANDIDATE row — one the controls never judged — is not deleted") {
    val dir = partition("t/runId=abc")
    purgeRun(Seq(row(dir, decision = PrimaryConstants.DECISION_CANDIDATE)))

    exists(dir) shouldBe true
  }

  test("a DELETE_WITH_WARNING row is deleted") {
    val dir = partition("t/runId=abc")
    purgeRun(Seq(row(dir, decision = PrimaryConstants.DECISION_WARN)))

    exists(dir) shouldBe false
  }

  // ---- PC13: already absent, and idempotence ---------------------------------------------------------

  test("a path that is already gone is SKIPPED_ABSENT, not a failure") {
    val dir = partition("t/runId=abc")
    val manifestRow = row(dir)
    FileUtils.deleteQuietly(dir.toFile)

    val result = purgeRun(Seq(manifestRow))
    result.records.head.status shouldBe PurgeExecutor.STATUS_SKIPPED_ABSENT
    result.status shouldBe "SUCCESS"
  }

  test("replaying the same manifest twice is safe") {
    // what makes recovering from a PARTIAL run possible at all
    val dir = partition("t/runId=abc")
    val manifestRow = row(dir)

    purgeRun(Seq(manifestRow)).records.head.status shouldBe PurgeExecutor.STATUS_TRASHED
    val second = purgeRun(Seq(manifestRow))
    second.records.head.status shouldBe PurgeExecutor.STATUS_SKIPPED_ABSENT
    second.status shouldBe "SUCCESS"
  }

  // ---- PC12: drift ------------------------------------------------------------------------------------

  test("an object whose size changed since the simulation is SKIPPED_DRIFT, not deleted") {
    val file = tmp.resolve("t/runId=abc/part-00000.orc")
    Files.createDirectories(file.getParent)
    Files.write(file, "original".getBytes(StandardCharsets.UTF_8))
    val manifestRow = row(file)
    Files.write(file, "rewritten, and longer than before".getBytes(StandardCharsets.UTF_8))

    val result = purgeRun(Seq(manifestRow))
    exists(file) shouldBe true
    result.records.head.status shouldBe PurgeExecutor.STATUS_SKIPPED_DRIFT
    result.records.head.errorMessage should include("changed since the simulation")
  }

  test("an object whose modification time changed is SKIPPED_DRIFT") {
    val dir = partition("t/runId=abc")
    val manifestRow = row(dir, modificationTime = new Timestamp(dir.toFile.lastModified() - 60000L))

    purgeRun(Seq(manifestRow)).records.head.status shouldBe PurgeExecutor.STATUS_SKIPPED_DRIFT
    exists(dir) shouldBe true
  }

  test("a partition directory whose files are newer than itself is NOT drift") {
    // the inventory records max(dir, files) as the unit's mtime; comparing against the directory's
    // own would make almost every partition look changed, skip it, and still report SUCCESS
    val dir = tmp.resolve("t/runId=abc")
    Files.createDirectories(dir)
    dir.toFile.setLastModified(System.currentTimeMillis() - 60000L)
    Files.write(dir.resolve("part-00000.orc"), "data".getBytes(StandardCharsets.UTF_8))

    val newest = math.max(dir.toFile.lastModified(),
      dir.resolve("part-00000.orc").toFile.lastModified())
    val manifestRow = row(dir).copy(modification_time = new Timestamp(newest))

    val result = purgeRun(Seq(manifestRow))
    result.records.head.status shouldBe PurgeExecutor.STATUS_TRASHED
    exists(dir) shouldBe false
  }

  test("a partition whose files were rewritten IS drift") {
    val dir = partition("t/runId=abc", content = "original")
    val manifestRow = row(dir)
    Files.write(dir.resolve("part-00000.orc"),
      "rewritten and longer".getBytes(StandardCharsets.UTF_8))

    purgeRun(Seq(manifestRow)).records.head.status shouldBe PurgeExecutor.STATUS_SKIPPED_DRIFT
    exists(dir) shouldBe true
  }

  // ---- Trash availability -----------------------------------------------------------------------------

  test("a TRASH deletion FAILS when Trash is unavailable rather than hard-deleting") {
    // silently falling back would destroy the recoverability the strategy was chosen for
    spark.sparkContext.hadoopConfiguration.setLong("fs.trash.interval", 0L)
    val dir = partition("t/runId=abc")

    val result = purgeRun(Seq(row(dir)))
    exists(dir) shouldBe true
    result.records.head.status shouldBe PurgeExecutor.STATUS_FAILED
    result.records.head.errorMessage should include("Trash is unavailable")
    result.status shouldBe "PARTIAL"
  }

  // ---- failure isolation ---------------------------------------------------------------------------

  test("one object's failure does not stop the others") {
    val ok = partition("t/runId=good")
    val bad = row(partition("t/runId=bad")).copy(strategy = "SHRED")

    val result = purgeRun(Seq(row(ok), bad))
    exists(ok) shouldBe false
    result.failed.size shouldBe 1
    result.removed.size shouldBe 1
    result.status shouldBe "PARTIAL"
  }

  test("stopOnFirstError halts the run and records the objects it did not reach") {
    val bad = row(partition("t/runId=bad")).copy(strategy = "SHRED")
    val untouched = partition("t/runId=later")

    val result = purgeRun(Seq(bad, row(untouched)), conf("stopOnFirstError = true"))
    exists(untouched) shouldBe true
    result.records.map(_.status) shouldBe
      Seq(PurgeExecutor.STATUS_FAILED, PurgeExecutor.STATUS_SKIPPED_ABORTED)
  }

  test("an unknown strategy fails the object instead of guessing what was meant") {
    val dir = partition("t/runId=abc")
    val result = purgeRun(Seq(row(dir).copy(strategy = "SHRED")))

    exists(dir) shouldBe true
    result.records.head.errorMessage should include("SHRED")
  }

  // ---- the account ------------------------------------------------------------------------------------

  test("every manifest row produces exactly one detail record") {
    val rows = Seq(row(partition("t/runId=a")),
      row(partition("t/runId=b"), decision = PrimaryConstants.DECISION_BLOCKED),
      row(partition("t/runId=c"), strategy = PrimaryConstants.STRATEGY_LOGICAL))

    purgeRun(rows).records.size shouldBe 3
  }

  test("the summary distinguishes a partial purge from a clean one") {
    purgeRun(Seq(row(partition("t/runId=a")))).summaryLine should include("PURGE SUCCESS")
    purgeRun(Seq(row(partition("t/runId=b")).copy(strategy = "SHRED"))).summaryLine should include("PURGE PARTIAL")
  }

  // ---- the Hive predicate ----------------------------------------------------------------------------

  test("a partition spec becomes a quoted predicate") {
    PurgeExecutor.partitionPredicate("runid=abc") shouldBe "`runid`='abc'"
    PurgeExecutor.partitionPredicate("runid=abc/scenario=FW") shouldBe "`runid`='abc', `scenario`='FW'"
  }

  test("a quote in a partition value is escaped, not concatenated into the statement") {
    // partition values come from a filesystem path, which is not a place to trust
    PurgeExecutor.partitionPredicate("runid=a'b") shouldBe "`runid`='a\\'b'"
  }
}

/** The manifest columns the executor reads, as a case class so a test can build one row by hand. */
final case class ManifestRow(path: String,
                             request_id: String,
                             database_name: String,
                             table_name: String,
                             partition_spec: String,
                             is_registered_partition: Boolean,
                             strategy: String,
                             decision: String,
                             size_bytes: Long,
                             num_files: Long,
                             modification_time: Timestamp)
