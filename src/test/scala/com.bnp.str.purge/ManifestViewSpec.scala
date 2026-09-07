package com.bnp.str.purge

import com.bnp.str.purge.mapping.ManifestView
import com.bnp.str.purge.reader.InventoryReader
import com.bnp.str.purge.utility.PrimaryConstants
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.LocalDate

/**
 * The manifest is what an approval is given against and what an execution replays, so its two
 * guarantees are tested directly: the fingerprint changes when — and only when — the approved set
 * changes, and the control columns start in a state that cannot delete anything.
 */
class ManifestViewSpec extends AnyFunSuite with Matchers with SparkTestSession {

  private val today = LocalDate.of(2026, 8, 28)
  private val mtime = Timestamp.valueOf("2023-04-01 10:00:00")

  private def unit(path: String, sizeBytes: Long = 100L, modified: Timestamp = mtime) =
    InventoryReader.PurgeUnit(path, "/data/rwa/t", PrimaryConstants.OBJECT_TYPE_LEAF_DIR,
      "/data/rwa", 1, sizeBytes, 1L, modified, None, "j03627", "grp", "rwxr-xr-x")

  private def frame(units: InventoryReader.PurgeUnit*): DataFrame = {
    import spark.implicits._
    spark.createDataset(units.toList).toDF()
  }

  // ---- the fingerprint ---------------------------------------------------------------------------

  test("the same set fingerprints the same, whatever the row order or the partitioning") {
    val a = frame(unit("/data/rwa/t/p=1"), unit("/data/rwa/t/p=2"), unit("/data/rwa/t/p=3"))
    val b = frame(unit("/data/rwa/t/p=3"), unit("/data/rwa/t/p=1"), unit("/data/rwa/t/p=2")).repartition(3)

    ManifestView.fingerprint(a) shouldBe ManifestView.fingerprint(b)
  }

  test("adding an object changes the fingerprint") {
    val before = frame(unit("/data/rwa/t/p=1"))
    val after = frame(unit("/data/rwa/t/p=1"), unit("/data/rwa/t/p=2"))

    ManifestView.fingerprint(after) should not be ManifestView.fingerprint(before)
  }

  test("removing an object changes the fingerprint") {
    val before = frame(unit("/data/rwa/t/p=1"), unit("/data/rwa/t/p=2"))
    val after = frame(unit("/data/rwa/t/p=1"))

    ManifestView.fingerprint(after) should not be ManifestView.fingerprint(before)
  }

  test("a partition REWRITTEN between simulation and execution changes the fingerprint") {
    // the reason the fingerprint covers physical state and not just identity: the same path holding
    // different data is not the object that was approved
    val approved = frame(unit("/data/rwa/t/p=1", sizeBytes = 100L))
    val resized = frame(unit("/data/rwa/t/p=1", sizeBytes = 101L))
    val touched = frame(unit("/data/rwa/t/p=1", modified = Timestamp.valueOf("2026-08-28 09:00:00")))

    ManifestView.fingerprint(resized) should not be ManifestView.fingerprint(approved)
    ManifestView.fingerprint(touched) should not be ManifestView.fingerprint(approved)
  }

  test("a change outside the fingerprinted columns does not invalidate an approval") {
    val approved = frame(unit("/data/rwa/t/p=1"))
    val reowned = frame(unit("/data/rwa/t/p=1").copy(owner_name = "someone_else", num_files = 7L))

    ManifestView.fingerprint(reowned) shouldBe ManifestView.fingerprint(approved)
  }

  test("an empty manifest has a stable fingerprint rather than no fingerprint") {
    val empty = frame()
    ManifestView.fingerprint(empty) should have length 64
    ManifestView.fingerprint(empty) shouldBe ManifestView.fingerprint(frame())
  }

  // ---- the manifest ------------------------------------------------------------------------------

  test("the manifest starts at CANDIDATE, which the executor never acts on") {
    val manifest = ManifestView.build(frame(unit("/data/rwa/t/p=1")), "run-1", "PRG-1", today, "fp")(spark)
    val row = manifest.head()

    row.getAs[String]("decision") shouldBe PrimaryConstants.DECISION_CANDIDATE
    PrimaryConstants.DELETABLE_DECISIONS should not contain PrimaryConstants.DECISION_CANDIDATE
  }

  test("the control columns exist from P2, so the schema never moves under an approved manifest") {
    val manifest = ManifestView.build(frame(unit("/data/rwa/t/p=1")), "run-1", "PRG-1", today, "fp")(spark)
    val row = manifest.head()

    row.getAs[Seq[String]]("controls_ko") shouldBe empty
    row.getAs[Seq[String]]("controls_warn") shouldBe empty
  }

  test("the run identity and the fingerprint travel on every row") {
    val manifest = ManifestView.build(
      frame(unit("/data/rwa/t/p=1"), unit("/data/rwa/t/p=2")), "run-1", "PRG-1", today, "fp-abc")(spark)

    manifest.select("run_id").distinct().collect().map(_.getString(0)) shouldBe Array("run-1")
    manifest.select("request_id").distinct().collect().map(_.getString(0)) shouldBe Array("PRG-1")
    manifest.select("manifest_fingerprint").distinct().collect().map(_.getString(0)) shouldBe Array("fp-abc")
    manifest.select("purge_date").distinct().collect().map(_.getString(0)) shouldBe Array("2026-08-28")
  }

  test("the write partition columns come last, as in run_history") {
    val manifest = ManifestView.build(frame(unit("/data/rwa/t/p=1")), "run-1", "PRG-1", today, "fp")(spark)
    manifest.columns.takeRight(2) shouldBe Array("purge_date", "run_id")
  }
}
