package com.bnp.str.purge.job

import com.bnp.str.purge.common.PrimaryRunner
import com.bnp.str.purge.control.{CheckConfig, CheckWriter}
import com.bnp.str.purge.mapping.ManifestView
import com.bnp.str.purge.reader.PrimaryReader
import com.bnp.str.purge.sessionmanager.{LocalDatabaseRegistrar, StrSparkSessionManager}
import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.bnp.str.purge.writer.PrimaryWriter
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.util.UUID

/**
 * P2 entry point, and the one behind TWIST's "Analyse" button: inventory the scope, select the
 * candidates, write the fingerprinted manifest. READ-ONLY.
 *
 * This driver has no code path that deletes, and it keeps that property permanently — at P4,
 * `MainDriver` gains the execution phase and this one does not. TWIST calls this class for a
 * simulation precisely so that a mistake in the conf, in the mode flag, or in the IHM cannot turn
 * an analysis into a deletion: the class the operator asked for simply has no such capability.
 *
 * Usage: `spark-submit --class com.bnp.str.purge.job.SimulationDriver <jar> <application.conf>`
 */
object SimulationDriver {

  private val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "Missing argument: path to the application.conf")
    val absoluteConfigPath = args(0)

    log.info(s"Start ${PrimaryConstants.APPLICATION_NAME} (${this.getClass.getName}) — simulation, read-only")

    val sparkSession = StrSparkSessionManager.fetchSparkSession(PrimaryConstants.APPLICATION_NAME)

    try {
      implicit val spark: SparkSession = sparkSession
      implicit val config: Config =
        ConfigFactory.parseReader(PrimaryUtilities.getHdfsReader(absoluteConfigPath)(sparkSession.sparkContext))

      // P4 replaces this with the run_history run id from PurgeAudit; until then a manifest still
      // needs an identity, because it is what an approval will be given against.
      // A laptop has an empty metastore; this fills it in so a database location resolves
      // the way it does on the cluster. A no-op anywhere else.
      LocalDatabaseRegistrar.register(config)

      val runId = UUID.randomUUID().toString
      log.info(s"Simulation runId=$runId, request=${PrimaryRunner.requestId(config)}, " +
        s"asOfDate=${PrimaryRunner.asOfDate(config)}")

      val primaryReader = new PrimaryReader()
      val primaryWriter = new PrimaryWriter()

      val outcome =
        new PrimaryRunner(primaryReader, PrimaryConstants.PURGE_MANIFEST).run_purge_runner(runId)
      val manifest = outcome.manifest.cache()

      log.info(ManifestView.summaryLine(manifest, outcome.manifestFingerprint))
      logByTable(manifest)

      // The report is written BEFORE the manifest, and a failure to write it fails the run. It is
      // the document the approval is given on, so a manifest sitting on HDFS with no report beside
      // it would look ready for approval while being unreviewable.
      val checks = CheckConfig.from(config)
      if (checks.htmlPath.nonEmpty)
        CheckWriter.writeHtml(checks.htmlPath, outcome.report)(sparkSession)
      else
        log.warn("controls.htmlPath is not set; no control report was written. TWIST has nothing " +
          "to show an approver for this run.")

      if (outputEnabled(config, PrimaryConstants.PURGE_MANIFEST))
        primaryWriter.write(manifest, PrimaryConstants.PURGE_MANIFEST)(sparkSession, config)
      else
        log.info(s"Output '${PrimaryConstants.PURGE_MANIFEST}' disabled (enable=false); manifest not written")

      log.info(s"End ${PrimaryConstants.APPLICATION_NAME} (${this.getClass.getName})")
    } catch {
      case e: Throwable =>
        log.error(s"${PrimaryConstants.APPLICATION_NAME} simulation failed: ${e.getMessage}", e)
        throw e
    }
  }

  private def outputEnabled(config: Config, tableName: String): Boolean = {
    val path = s"${PrimaryConstants.APP_CONF}.$tableName"
    config.hasPath(path) && PrimaryUtilities.getBooleanOr(config.getConfig(path), "enable", default = false)
  }

  /**
   * What the manifest holds, per table and per reason. This is the breakdown a person reads before
   * deciding whether the simulation matched their intent — an aggregate total hides the case where
   * one unexpected table contributes everything.
   */
  private def logByTable(manifest: org.apache.spark.sql.DataFrame): Unit = {
    // `functions.` rather than a wildcard import: `functions.log` would shadow this object's logger
    import org.apache.spark.sql.functions.{coalesce, col, count, lit, sum}

    val rows = manifest
      .groupBy(col("version_group"), col("decision"), col("selection_source"), col("policy_id"))
      .agg(count(lit(1)).as("objects"), coalesce(sum(col("size_bytes")), lit(0L)).as("bytes"))
      .orderBy(col("bytes").desc)
      .limit(50)
      .collect()

    if (rows.isEmpty) {
      log.warn("[manifest] EMPTY: no object is beyond its retention and nothing was explicitly selected")
      return
    }

    rows.foreach { row =>
      log.info(f"[manifest] ${row.getAs[String]("version_group")}%-45s " +
        f"${row.getAs[String]("decision")}%-20s " +
        f"${row.getAs[String]("selection_source")}%-13s " +
        f"policy=${Option(row.getAs[String]("policy_id")).getOrElse("<none>")}%-14s " +
        f"objects=${row.getAs[Long]("objects")}%6d " +
        f"size=${PrimaryUtilities.humanBytes(row.getAs[Long]("bytes"))}%12s")
    }
  }
}
