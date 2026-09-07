package com.bnp.str.purge.job

import com.bnp.str.purge.reader.{CatalogReader, InventoryReader}
import com.bnp.str.purge.sessionmanager.StrSparkSessionManager
import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.bnp.str.purge.writer.PrimaryWriter
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, count, sum}
import org.slf4j.LoggerFactory

/**
 * P1 entry point: inventory a scope and write it as ORC. READ-ONLY — this driver has no code path
 * that removes anything, and that is the point of shipping it first. It lets the retention policies
 * be written and argued about against the real content of Promethee, months before any destructive
 * class exists.
 *
 * It writes two outputs, each guarded by its own `enable` flag:
 *   - `purge_inventory` — what is on HDFS under `scan.roots` (see [[InventoryReader]]);
 *   - `purge_catalog`   — what the metastore believes exists in `scan.databases` (see [[CatalogReader]]).
 *
 * Usage: `spark-submit --class com.bnp.str.purge.job.InventoryDriver <jar> <application.conf>`
 *
 * At P4 this becomes `MainDriver`, which runs the same inventory, then the selection, the controls
 * and — only behind an approved manifest — the deletion.
 */
object InventoryDriver {

  private val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "Missing argument: path to the application.conf")
    val absoluteConfigPath = args(0)

    log.info(s"Start ${PrimaryConstants.APPLICATION_NAME} (${this.getClass.getName}) — inventory only, read-only")

    val sparkSession = StrSparkSessionManager.fetchSparkSession(PrimaryConstants.APPLICATION_NAME)

    try {
      val configReader = PrimaryUtilities.getHdfsReader(absoluteConfigPath)(sparkSession.sparkContext)
      implicit val config: Config = ConfigFactory.parseReader(configReader)
      implicit val spark: org.apache.spark.sql.SparkSession = sparkSession

      val primaryWriter = new PrimaryWriter()

      // ---- HDFS inventory ----
      if (outputEnabled(config, PrimaryConstants.PURGE_INVENTORY)) {
        val inventory = new InventoryReader().read().cache()
        logInventorySummary(inventory)
        primaryWriter.write(inventory, PrimaryConstants.PURGE_INVENTORY)(sparkSession, config)
        inventory.unpersist()
      } else {
        log.info(s"Output '${PrimaryConstants.PURGE_INVENTORY}' disabled (enable=false); skipping the HDFS scan")
      }

      // ---- metastore view of the same scope ----
      if (outputEnabled(config, PrimaryConstants.PURGE_CATALOG)) {
        val catalog = new CatalogReader().read()
        primaryWriter.write(catalog, PrimaryConstants.PURGE_CATALOG)(sparkSession, config)
      } else {
        log.info(s"Output '${PrimaryConstants.PURGE_CATALOG}' disabled (enable=false); skipping the catalog read")
      }

      log.info(s"End ${PrimaryConstants.APPLICATION_NAME} (${this.getClass.getName})")
    } catch {
      case e: Throwable =>
        log.error(s"${PrimaryConstants.APPLICATION_NAME} failed: ${e.getMessage}", e)
        throw e
    }
  }

  /** An output runs only when its config block exists AND `enable = true` — absent means off. */
  private def outputEnabled(config: Config, tableName: String): Boolean = {
    val path = s"${PrimaryConstants.APP_CONF}.$tableName"
    config.hasPath(path) && PrimaryUtilities.getBooleanOr(config.getConfig(path), "enable", default = false)
  }

  /**
   * One log line per object type: total objects, files and bytes. This is the number the team will
   * read first — how much is actually down there — so it is logged before the write rather than
   * left to be discovered by querying the ORC afterwards.
   */
  private def logInventorySummary(inventory: org.apache.spark.sql.DataFrame): Unit = {
    val summary = inventory
      .groupBy(col("object_type"))
      .agg(count("*").as("objects"), sum("num_files").as("files"), sum("size_bytes").as("bytes"))
      .collect()

    if (summary.isEmpty) log.warn("Inventory is EMPTY: the scan roots exist but hold nothing")
    else summary.foreach { row =>
      val bytes = Option(row.getAs[Any]("bytes")).map(_.toString.toLong).getOrElse(0L)
      log.info(f"[inventory] ${row.getAs[String]("object_type")}%-14s " +
        f"objects=${row.getAs[Long]("objects")}%8d " +
        f"files=${Option(row.getAs[Any]("files")).map(_.toString).getOrElse("0")}%10s " +
        f"size=${PrimaryUtilities.humanBytes(bytes)}%12s")
    }
  }
}
