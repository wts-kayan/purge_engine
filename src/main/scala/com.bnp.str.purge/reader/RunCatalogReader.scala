package com.bnp.str.purge.reader

import com.bnp.str.purge.engine.EngineDescriptor
import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Every run an engine has left on disk, with what it occupies.
 *
 * This is what the IHM's scope screen is built on, and it is the one thing the engine could not do
 * before P5: a person can only choose a run to purge from a list of runs, and until now the engine
 * had to be handed a run's configuration before it could say anything at all. Asking TWIST to
 * produce that list instead would mean asking it to walk HDFS, which is exactly the work this engine
 * exists to do — and would give two different answers to "how big is that run?".
 *
 * The catalogue is built from the FILESYSTEM and then annotated from the engine's history, in that
 * order and never the reverse. What is on disk is what a purge would remove; the history says what
 * the engine believes it produced. Where the two disagree the disagreement is the interesting part:
 *
 *  - on disk, absent from the history — a run the engine lost track of, and the most likely thing
 *    anyone actually wants to purge;
 *  - in the history, absent from disk — already purged, or never written.
 *
 * A catalogue built from the history alone would show the second and miss the first.
 */
class RunCatalogReader()(implicit sparkSession: SparkSession, conf: Config) {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val engineConfig: Config =
    conf.getConfig(s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.ENGINE}")

  private val descriptor: EngineDescriptor =
    EngineDescriptor.of(PrimaryUtilities.getStringOr(engineConfig, "name", ""))

  /**
   * One row per run found under the scan roots.
   *
   * The inventory does the walking — the same walk, with the same containment checks, that a purge
   * would use — and the runs are recovered from each path according to the engine's granularity.
   * Objects that carry no run id (a table's own directory under a partition-granular engine, a stray
   * file, the engine's history table) are simply not runs and are left out.
   */
  def read(): DataFrame = {
    val inventory = new InventoryReader().read()
    val partitionKey = descriptor.partitionKey
    val tableGranular = descriptor.isTableGranular

    // Where a run's identity sits in a path is decided by the granularity, so both readings are
    // spelled out rather than one assumed. For a partition-granular engine the run is the
    // `runId=<uuid>` directory inside each table; for a table-granular one the run IS the table, so
    // the table directory's own name identifies it. Reading a table-granular layout with the
    // partition rule finds no `runId=` segment anywhere, drops every row, and reports an empty
    // catalogue — a scope screen showing nothing to purge, which is indistinguishable from an
    // engine that has nothing on disk.
    val runId = udf { path: String =>
      if (tableGranular) PrimaryUtilities.baseName(path)
      else PrimaryUtilities.extractPartitionValue(path, partitionKey).orNull
    }

    val tableOf = udf { path: String =>
      if (tableGranular) PrimaryUtilities.baseName(path)
      else PrimaryUtilities.baseName(PrimaryUtilities.parentPath(path))
    }

    val onDisk = inventory
      .withColumn("run_id", runId(col("path")))
      .where(col("run_id").isNotNull)
      .withColumn("table_name", tableOf(col("path")))
      .groupBy(col("run_id"))
      .agg(
        countDistinct(col("table_name")).as("table_count"),
        count(lit(1)).as("object_count"),
        coalesce(sum(col("size_bytes")), lit(0L)).as("size_bytes"),
        coalesce(sum(col("num_files")), lit(0L)).as("file_count"),
        min(col("modification_time")).as("first_written"),
        max(col("modification_time")).as("last_written"))
      .withColumn("engine", lit(descriptor.name))

    val catalog = annotateFromHistory(onDisk)
    log.info(s"Run catalogue: ${catalog.count()} run(s) of '${descriptor.name}' found on disk")
    catalog
  }

  /**
   * Add what the engine's own history knows about each run — its name, its status, who launched it.
   *
   * A LEFT join, deliberately: a run present on disk stays in the catalogue whether or not the
   * history has heard of it. `known_to_history = false` is a finding, not a reason to hide the row.
   */
  private def annotateFromHistory(onDisk: DataFrame): DataFrame =
    new EngineRunReader().historyForCatalog() match {
      case None =>
        log.warn("No engine run history available; the catalogue reports what is on disk and " +
          "nothing about the state of each run")
        onDisk
          .withColumn("known_to_history", lit(false))
          .withColumn("history_status", lit(""))
          .withColumn("unfinished", lit(false))
          .withColumn("used_conf", lit(""))
          .withColumn("run_type", lit(""))
          .withColumn("real_user_id", lit(""))
          .withColumn("started_at", lit(null).cast("timestamp"))
          .withColumn("ended_at", lit(null).cast("timestamp"))

      case Some(history) =>
        val renamed = history.withColumnRenamed("run_id", "h_run_id")
        onDisk
          .join(broadcast(renamed), col("run_id") === col("h_run_id"), "left")
          .withColumn("known_to_history", col("h_run_id").isNotNull)
          .withColumn("history_status", coalesce(col("history_status"), lit("")))
          .withColumn("unfinished", coalesce(col("unfinished"), lit(false)))
          .withColumn("used_conf", coalesce(col("used_conf"), lit("")))
          .withColumn("run_type", coalesce(col("run_type"), lit("")))
          .withColumn("real_user_id", coalesce(col("real_user_id"), lit("")))
          .drop("h_run_id", "history_rows")
    }
}

object RunCatalogReader {

  /** The columns the IHM reads, in the order a scope screen shows them. */
  val COLUMNS = Seq("engine", "run_id", "run_type", "real_user_id", "table_count", "object_count",
    "size_bytes", "file_count", "first_written", "last_written", "started_at", "ended_at",
    "known_to_history", "history_status", "unfinished", "used_conf")
}
