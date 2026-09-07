package com.bnp.str.purge

import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The path helpers are the foundation the guard stands on: every containment decision in the engine
 * is one of these calls. They are tested first, and hardest, because a `startsWith` that looks
 * right is exactly how a purge escapes its allowed root.
 */
class PrimaryUtilitiesSpec extends AnyFunSuite with Matchers {

  private val allowed = Seq("/data/promethee/str/rwa", "/data/promethee/str/ifrs9")

  // ---- normalizePath -------------------------------------------------------------------------

  test("normalizePath drops the scheme and authority") {
    PrimaryUtilities.normalizePath("hdfs://promethee-nn/data/promethee/str/rwa") shouldBe "/data/promethee/str/rwa"
  }

  test("normalizePath collapses duplicate separators and drops the trailing one") {
    PrimaryUtilities.normalizePath("/data//promethee///str/rwa/") shouldBe "/data/promethee/str/rwa"
  }

  test("normalizePath leaves the filesystem root as a single separator") {
    PrimaryUtilities.normalizePath("/") shouldBe "/"
  }

  test("normalizePath is empty for null or blank") {
    PrimaryUtilities.normalizePath(null) shouldBe ""
    PrimaryUtilities.normalizePath("   ") shouldBe ""
  }

  // ---- depth / naming ------------------------------------------------------------------------

  test("pathDepth counts non-empty segments") {
    PrimaryUtilities.pathDepth("/data/promethee/str/rwa") shouldBe 4
    PrimaryUtilities.pathDepth("/data") shouldBe 1
    PrimaryUtilities.pathDepth("/") shouldBe 0
  }

  test("baseName and parentPath split a path") {
    PrimaryUtilities.baseName("/data/promethee/str/rwa/as_of_date=2023-03-31") shouldBe "as_of_date=2023-03-31"
    PrimaryUtilities.parentPath("/data/promethee/str/rwa/as_of_date=2023-03-31") shouldBe "/data/promethee/str/rwa"
    PrimaryUtilities.parentPath("/data") shouldBe "/"
  }

  // ---- containment ---------------------------------------------------------------------------

  test("isUnderRoot accepts the root itself and anything strictly inside it") {
    PrimaryUtilities.isUnderRoot("/data/promethee/str/rwa", "/data/promethee/str/rwa") shouldBe true
    PrimaryUtilities.isUnderRoot("/data/promethee/str/rwa/ts_ead_fwd", "/data/promethee/str/rwa") shouldBe true
  }

  test("isUnderRoot rejects a sibling whose name merely starts with the root") {
    // the whole reason the separator is part of the comparison
    PrimaryUtilities.isUnderRoot("/data/promethee/str/rwa-archive", "/data/promethee/str/rwa") shouldBe false
    PrimaryUtilities.isUnderRoot("/data/promethee/str/rwabis", "/data/promethee/str/rwa") shouldBe false
  }

  test("isUnderRoot compares canonical forms, so spelling does not matter") {
    PrimaryUtilities.isUnderRoot("hdfs://nn/data/promethee//str/rwa/x/", "/data/promethee/str/rwa") shouldBe true
  }

  test("isUnderAnyRoot is true when at least one root contains the path") {
    PrimaryUtilities.isUnderAnyRoot("/data/promethee/str/ifrs9/t", allowed) shouldBe true
    PrimaryUtilities.isUnderAnyRoot("/data/promethee/str/other/t", allowed) shouldBe false
  }

  // ---- unsafePathReason ----------------------------------------------------------------------

  test("unsafePathReason accepts a legitimate target") {
    PrimaryUtilities.unsafePathReason("/data/promethee/str/rwa/ts_ead_fwd", allowed) shouldBe None
  }

  test("unsafePathReason refuses the forbidden paths") {
    PrimaryConstants.FORBIDDEN_PATHS.foreach { forbidden =>
      PrimaryUtilities.unsafePathReason(forbidden, Seq.empty) should not be empty
    }
  }

  test("unsafePathReason refuses a path shallower than the minimum depth") {
    PrimaryUtilities.unsafePathReason("/data/promethee/str", Seq.empty).get should include("depth")
  }

  test("unsafePathReason refuses traversal and unresolved wildcards") {
    PrimaryUtilities.unsafePathReason("/data/promethee/str/rwa/../../..", allowed) should not be empty
    PrimaryUtilities.unsafePathReason("/data/promethee/str/rwa/*", allowed).get should include("wildcard")
  }

  test("unsafePathReason refuses a path outside the allowed roots") {
    PrimaryUtilities.unsafePathReason("/data/promethee/str/other/t", allowed).get should include("outside")
  }

  test("unsafePathReason with no allowed roots still applies the structural rules") {
    // an empty allowedRoots means "no whitelist configured", never "everything is allowed"
    PrimaryUtilities.unsafePathReason("/data/promethee/str/rwa/t", Seq.empty) shouldBe None
    PrimaryUtilities.unsafePathReason("/tmp", Seq.empty) should not be empty
  }

  // ---- partition helpers ---------------------------------------------------------------------

  test("extractPartitionValue finds a Hive-style partition value anywhere in the path") {
    PrimaryUtilities.extractPartitionValue(
      "/data/t/as_of_date=2023-03-31/part-00000.orc", "as_of_date") shouldBe Some("2023-03-31")
  }

  test("extractPartitionValue takes the innermost occurrence") {
    PrimaryUtilities.extractPartitionValue(
      "/data/t/as_of_date=2023-03-31/sub/as_of_date=2024-06-30/f.orc", "as_of_date") shouldBe Some("2024-06-30")
  }

  test("extractPartitionValue matches the key whatever its case, and keeps the value's") {
    // the projection engine writes `runId=` on disk; the metastore stores `runid`. A case-sensitive
    // match would not recognise the partition as a run: no error, no row, nothing to purge.
    val uuid = "9df8cf3a-C2FA-4bfd-9068-aa6587ba84cf"
    PrimaryUtilities.extractPartitionValue(s"/data/t/runId=$uuid", "runid") shouldBe Some(uuid)
    PrimaryUtilities.extractPartitionValue(s"/data/t/runid=$uuid", "runId") shouldBe Some(uuid)
    PrimaryUtilities.extractPartitionValue(s"/data/t/RUNID=$uuid", "runId") shouldBe Some(uuid)
  }

  test("extractPartitionValue returns None for an absent or blank column") {
    PrimaryUtilities.extractPartitionValue("/data/t/part-0.orc", "as_of_date") shouldBe None
    PrimaryUtilities.extractPartitionValue("/data/t/as_of_date=/f.orc", "as_of_date") shouldBe None
    PrimaryUtilities.extractPartitionValue("/data/t/as_of_date=2023-03-31", "") shouldBe None
  }

  test("partitionSpecOf rebuilds the full spec in path order") {
    PrimaryUtilities.partitionSpecOf(
      "/data/t/as_of_date=2023-03-31/scenario=FW/part-0.orc") shouldBe "as_of_date=2023-03-31/scenario=FW"
    PrimaryUtilities.partitionSpecOf("/data/t/part-0.orc") shouldBe ""
  }

  // ---- humanBytes ----------------------------------------------------------------------------

  test("humanBytes renders a size a person can read") {
    PrimaryUtilities.humanBytes(512L) shouldBe "512 B"
    PrimaryUtilities.humanBytes(1536L) shouldBe "1.5 KiB"
    PrimaryUtilities.humanBytes(5L * 1024 * 1024 * 1024 * 1024) shouldBe "5.0 TiB"
  }
}
