package com.bnp.str.purge.job

import com.bnp.str.purge.reader.RunCatalogReader
import com.bnp.str.purge.sessionmanager.{LocalDatabaseRegistrar, StrSparkSessionManager}
import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.bnp.str.purge.writer.PrimaryWriter
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Lists the runs of an engine that are still on disk. READ-ONLY.
 *
 * This is what the IHM calls to fill its scope screen: a person picks a run to purge from this
 * list, and TWIST then submits [[SimulationDriver]] with that run's configuration. It has no
 * destructive code, and — like [[SimulationDriver]] — it keeps that property permanently rather
 * than being a safe mode of a class that can also delete.
 *
 * Usage: `spark-submit --class com.bnp.str.purge.job.RunCatalogDriver <jar> <application.conf>`
 */
object RunCatalogDriver {

  private val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "Missing argument: path to the application.conf")
    val absoluteConfigPath = args(0)

    log.info(s"Start ${PrimaryConstants.APPLICATION_NAME} (${this.getClass.getName}) — run catalogue, read-only")

    val sparkSession = StrSparkSessionManager.fetchSparkSession(PrimaryConstants.APPLICATION_NAME)

    try {
      implicit val spark: SparkSession = sparkSession
      implicit val config: Config =
        ConfigFactory.parseReader(PrimaryUtilities.getHdfsReader(absoluteConfigPath)(sparkSession.sparkContext))

      // A laptop has an empty metastore; this fills it in so a database location resolves
      // the way it does on the cluster. A no-op anywhere else.
      LocalDatabaseRegistrar.register(config)

      val catalog = new RunCatalogReader().read()
        .select(RunCatalogReader.COLUMNS.map(col): _*)
        .orderBy(col("last_written").desc)
        .cache()

      logCatalog(catalog)

      if (outputEnabled(config, PrimaryConstants.PURGE_RUN_CATALOG))
        new PrimaryWriter().write(catalog, PrimaryConstants.PURGE_RUN_CATALOG)(sparkSession, config)
      else
        log.info(s"Output '${PrimaryConstants.PURGE_RUN_CATALOG}' disabled (enable=false); " +
          "the catalogue is in this log only")

      log.info(s"End ${PrimaryConstants.APPLICATION_NAME} (${this.getClass.getName})")
    } catch {
      case e: Throwable =>
        log.error(s"${PrimaryConstants.APPLICATION_NAME} run catalogue failed: ${e.getMessage}", e)
        throw e
    }
  }

  /**
   * The catalogue as a person would read it, newest run first.
   *
   * Runs the engine's history has never heard of are called out: they are the ones most likely to
   * be worth purging, and the ones a listing built from the history alone would never show.
   */
  private def logCatalog(catalog: DataFrame): Unit = {
    val rows = catalog.limit(200).collect()

    if (rows.isEmpty) {
      log.warn("[catalogue] no run found under the scan roots")
      return
    }

    rows.foreach { row =>
      val known = if (row.getAs[Boolean]("known_to_history")) row.getAs[String]("history_status") else "NOT IN HISTORY"
      log.info(f"[catalogue] ${row.getAs[String]("run_id")}%-38s " +
        f"tables=${row.getAs[Long]("table_count")}%4d " +
        f"objects=${row.getAs[Long]("object_count")}%5d " +
        f"size=${PrimaryUtilities.humanBytes(row.getAs[Long]("size_bytes"))}%12s " +
        f"last=${row.getAs[java.sql.Timestamp]("last_written")}%-21s $known")
    }

    val orphans = rows.count(!_.getAs[Boolean]("known_to_history"))
    if (orphans > 0)
      log.warn(s"[catalogue] $orphans run(s) on disk that the engine's history does not know about")
  }

  private def outputEnabled(config: Config, tableName: String): Boolean = {
    val path = s"${PrimaryConstants.APP_CONF}.$tableName"
    config.hasPath(path) && PrimaryUtilities.getBooleanOr(config.getConfig(path), "enable", default = false)
  }
}
