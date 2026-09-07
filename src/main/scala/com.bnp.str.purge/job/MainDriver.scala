package com.bnp.str.purge.job

import com.bnp.str.purge.audit.PurgeAudit
import com.bnp.str.purge.common.{PrimaryRunner, PurgeOutcome}
import com.bnp.str.purge.control.{CheckConfig, CheckWriter}
import com.bnp.str.purge.mapping.ManifestView
import com.bnp.str.purge.purge.{PurgeExecutor, PurgeGuard}
import com.bnp.str.purge.reader.{ManifestReader, PrimaryReader}
import com.bnp.str.purge.sessionmanager.{LocalDatabaseRegistrar, StrSparkSessionManager}
import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.bnp.str.purge.writer.{PrimaryWriter, PurgeDetailWriter}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * The purge engine's entry point, and the only class in it that can delete.
 *
 * `request.mode` decides what it does:
 *
 *  - `SIMULATE` — inventory, select, control, write the manifest and the report. Nothing deleted.
 *    Identical to [[SimulationDriver]], which exists so that TWIST's "Analyse" button calls a class
 *    that has no destructive code at all rather than this one in a safe mode.
 *  - `EXECUTE` — replay the manifest of `request.manifestRunId` and delete what it says. Builds
 *    nothing: what was simulated, reviewed and named is what is executed.
 *  - `SIMULATE_AND_EXECUTE` — both, in one submission, with nobody looking in between. Legitimate
 *    now that the business decided against an approval step (Q11), and still the mode that gives up
 *    the one thing the two-phase design was for, so it says so loudly in the log.
 *
 * Usage: `spark-submit --class com.bnp.str.purge.job.MainDriver <jar> <application.conf>`
 */
object MainDriver {

  private val log = LoggerFactory.getLogger(this.getClass)

  val MODE_SIMULATE = "SIMULATE"
  val MODE_EXECUTE = "EXECUTE"
  val MODE_SIMULATE_AND_EXECUTE = "SIMULATE_AND_EXECUTE"
  val MODES = Seq(MODE_SIMULATE, MODE_EXECUTE, MODE_SIMULATE_AND_EXECUTE)

  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "Missing argument: path to the application.conf")
    val absoluteConfigPath = args(0)

    val sparkSession = StrSparkSessionManager.fetchSparkSession(PrimaryConstants.APPLICATION_NAME)
    implicit val spark: SparkSession = sparkSession
    implicit val config: Config =
      ConfigFactory.parseReader(PrimaryUtilities.getHdfsReader(absoluteConfigPath)(sparkSession.sparkContext))

    // A laptop has an empty metastore; this fills it in so a database location resolves
    // the way it does on the cluster. A no-op anywhere else.
    LocalDatabaseRegistrar.register(config)

    val mode = modeOf(config)
    val primaryReader = new PrimaryReader()

    log.info(s"Start ${PrimaryConstants.APPLICATION_NAME} (${this.getClass.getName}) mode=$mode " +
      s"request=${PrimaryRunner.requestId(config)} asOfDate=${PrimaryRunner.asOfDate(config)}")

    // Opened before anything else so that a run which dies mid-flight still leaves a RUNNING row —
    // for a job that deletes, "we do not know whether it finished" must be visible, not absent.
    val audit = PurgeAudit.start(config, absoluteConfigPath, primaryReader.engineRuns.runs)
    log.info(s"Run audit started: runId=${audit.runId}")

    try {
      val manifest =
        if (mode == MODE_EXECUTE) replay(config)
        else simulate(primaryReader, audit.runId)

      if (mode != MODE_SIMULATE) {
        if (mode == MODE_SIMULATE_AND_EXECUTE)
          log.warn("mode = SIMULATE_AND_EXECUTE: the manifest is executed in the same submission " +
            "that built it, so no one sees it in between. Use SIMULATE then EXECUTE to keep that " +
            "look; this mode only skips the pause, never the controls or the guard.")
        purge(manifest, audit.runId, config)
      } else {
        log.info("mode = SIMULATE: nothing deleted. Execute with " +
          s"request.mode = $MODE_EXECUTE and request.manifestRunId = ${audit.runId}")
      }

      audit.succeeded()
      log.info(s"End ${PrimaryConstants.APPLICATION_NAME} (${this.getClass.getName})")
    } catch {
      case e: Throwable =>
        audit.failed(e)
        log.error(s"${PrimaryConstants.APPLICATION_NAME} run ${audit.runId} FAILED: ${e.getMessage}", e)
        throw e
    }
  }

  // ---------------------------------------------------------------------------------------------

  /** Phase one: build the manifest, judge it, write both it and the report. */
  private def simulate(primaryReader: PrimaryReader, runId: String)
                      (implicit spark: SparkSession, config: Config): DataFrame = {
    val outcome: PurgeOutcome =
      new PrimaryRunner(primaryReader, PrimaryConstants.PURGE_MANIFEST).run_purge_runner(runId)
    val manifest = outcome.manifest.cache()

    log.info(ManifestView.summaryLine(manifest, outcome.manifestFingerprint))

    val checks = CheckConfig.from(config)
    if (checks.htmlPath.nonEmpty) CheckWriter.writeHtml(checks.htmlPath, outcome.report)
    else log.warn("controls.htmlPath is not set; no control report was written for this run")

    if (outputEnabled(config, PrimaryConstants.PURGE_MANIFEST))
      new PrimaryWriter().write(manifest, PrimaryConstants.PURGE_MANIFEST)(spark, config)
    else
      log.warn(s"${PrimaryConstants.PURGE_MANIFEST}.enable = false: the manifest is NOT persisted, " +
        s"so this run cannot be replayed with mode = $MODE_EXECUTE")

    manifest
  }

  /** Phase two, first half: the manifest that was written, not a new one. */
  private def replay(config: Config)(implicit spark: SparkSession): DataFrame = {
    val request = config.getConfig(s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.REQUEST}")
    new ManifestReader()(spark, config).read(PrimaryUtilities.getStringOr(request, "manifestRunId", ""))
  }

  /** Phase two, second half: the guard, and then the only deletion in the engine. */
  private def purge(manifest: DataFrame, runId: String, config: Config)
                   (implicit spark: SparkSession): Unit = {
    PurgeGuard.assertSafe(manifest)(spark, config)

    val request = config.getConfig(s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.REQUEST}")
    val requester = PrimaryUtilities.getStringOr(request, "requester", "")

    val result = new PurgeExecutor()(spark, config).execute(manifest, runId, requester)
    PurgeDetailWriter.write(result, PrimaryRunner.asOfDate(config).toString)(spark, config)

    // A partial purge must not report as a clean one, and the operator has to know that re-running
    // the same manifest is the safe way to finish it: every removed object comes back SKIPPED_ABSENT.
    if (result.failed.nonEmpty)
      throw new IllegalStateException(
        s"${result.failed.size} object(s) failed to purge; the run is PARTIAL. " +
          s"Re-running manifest $runId is safe and will retry only what is left. First failures: " +
          result.failed.take(5).map(r => s"${r.path} (${r.errorMessage})").mkString("; "))
  }

  private[str] def modeOf(config: Config): String = {
    val path = s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.REQUEST}"
    val request = if (config.hasPath(path)) config.getConfig(path) else ConfigFactory.empty()
    val mode = PrimaryUtilities.getStringOr(request, "mode", MODE_SIMULATE).trim.toUpperCase

    // An unreadable mode is never treated as EXECUTE, and never silently downgraded to SIMULATE
    // either: a typo must be corrected, not interpreted.
    require(MODES.contains(mode),
      s"Unknown $path.mode '$mode'. Expected one of: ${MODES.mkString(", ")}")
    mode
  }

  private def outputEnabled(config: Config, tableName: String): Boolean = {
    val path = s"${PrimaryConstants.APP_CONF}.$tableName"
    config.hasPath(path) && PrimaryUtilities.getBooleanOr(config.getConfig(path), "enable", default = false)
  }
}
