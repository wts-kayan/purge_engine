package com.bnp.str.purge.mapping

import com.bnp.str.purge.utility.PrimaryConstants
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

/**
 * Turns the candidate set into THE MANIFEST: the frozen, fingerprinted list an approval is given
 * against and an execution replays.
 *
 * The manifest is the engine's contract with the person who approves. Everything that makes it
 * trustworthy lives here:
 *
 *  - the control columns exist from P2 on, so the schema never changes under an approved manifest.
 *    `decision` starts at CANDIDATE — a real state meaning "selected, not yet judged" — and the P4
 *    executor only ever acts on DELETE / DELETE_WITH_WARNING. A manifest that never reached the
 *    controls therefore deletes nothing, by construction rather than by care.
 *  - the fingerprint is what the approval token is computed over, and what execution re-checks
 *    (PC12). It covers each object's identity AND its physical state — path, size, modification
 *    time — so a partition rewritten between the simulation and the execution invalidates the
 *    approval instead of being deleted in its new, unreviewed form.
 */
object ManifestView {

  /** Columns whose values the fingerprint is computed over, in this order. */
  val FINGERPRINT_COLUMNS = Seq("path", "size_bytes", "modification_time")

  /**
   * Fingerprint of a candidate set: each row is hashed, the hashes are sorted, and the sorted list
   * is hashed again.
   *
   * Hashing per row first, rather than sorting the raw `path|size|mtime` strings, keeps the memory
   * bounded by the number of rows times 64 bytes instead of by total path length — a scope of tens
   * of thousands of long HDFS paths otherwise builds a single very large array in one task. Sorting
   * makes the result independent of partitioning, so the same scope always fingerprints the same.
   */
  def fingerprint(candidates: DataFrame): String = {
    val rowHash = sha2(
      concat_ws("|", FINGERPRINT_COLUMNS.map(c => coalesce(col(c).cast("string"), lit("<null>"))): _*), 256)

    candidates
      .select(rowHash.as("row_hash"))
      .agg(sort_array(collect_list(col("row_hash"))).as("row_hashes"))
      .select(sha2(concat_ws("\n", col("row_hashes")), 256).as("fingerprint"))
      .head()
      .getAs[String]("fingerprint")
  }

  /**
   * The manifest: the candidates, plus the run identity, the control columns the P3 mapper fills,
   * and the fingerprint carried on every row so a single row read back out is still traceable to
   * the exact approved set.
   *
   * `purge_date` and `run_id` are the last two columns because they are the write partitioning —
   * the same convention as the shared `run_history`.
   */
  def build(candidates: DataFrame,
            runId: String,
            requestId: String,
            today: LocalDate,
            manifestFingerprint: String)(implicit sparkSession: SparkSession): DataFrame =
    // ONE select rather than eight chained withColumn calls. Each withColumn re-analyses the whole
    // plan, and the plan underneath is the candidate-selection query — five CTEs, four window
    // functions, three joins. Chaining them makes query PLANNING, not execution, the dominant cost
    // of a simulation: it grows with the depth of the tree, so it gets worse exactly where the
    // engine is doing its real work.
    candidates.select(
      col("*"),
      lit(requestId).as("request_id"),
      lit(PrimaryConstants.DECISION_CANDIDATE).as("decision"),
      array().cast("array<string>").as("controls_ko"),
      array().cast("array<string>").as("controls_warn"),
      lit(manifestFingerprint).as("manifest_fingerprint"),
      current_timestamp().as("generated_at"),
      lit(today.toString).as("purge_date"),
      lit(runId).as("run_id"))

  /**
   * One line for the run log and, later, the report header. Deliberately counts the objects and the
   * bytes SEPARATELY from the write: the number a person sanity-checks must come from the frame
   * that was actually built, not from a re-read of what landed on disk.
   */
  def summaryLine(manifest: DataFrame, fingerprint: String): String = {
    val row = manifest
      .agg(
        count(lit(1)).as("objects"),
        coalesce(sum(col("size_bytes")), lit(0L)).as("bytes"),
        countDistinct(col("version_group")).as("groups"))
      .head()

    val objects = row.getAs[Long]("objects")
    val bytes = row.getAs[Long]("bytes")
    val groups = row.getAs[Long]("groups")

    s"MANIFEST - $objects candidate(s) across $groups table(s)/folder(s), " +
      s"${com.bnp.str.purge.utility.PrimaryUtilities.humanBytes(bytes)} would be freed; " +
      s"fingerprint=${fingerprint.take(16)}... (nothing deleted: this run only simulates)"
  }
}
