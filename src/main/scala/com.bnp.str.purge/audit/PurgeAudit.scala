package com.bnp.str.purge.audit

import com.bnp.str.purge.common.PrimaryRunner
import com.bnp.str.purge.engine.EngineRun
import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.bnp.str.utilities.audit.RunAudit
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

/**
 * Purge-specific wiring for the shared, module-agnostic [[RunAudit]].
 *
 * Same shape as `TseadfwdAudit` in the transform engine: it pulls this module's `run_history`
 * metadata out of the run configuration so the driver only has to call [[start]], and keeps purge
 * config-key knowledge out of both the driver and the shared audit package.
 *
 *   projection_dates <- request.asOfDate (the date every retention was measured against)
 *   scenarios        <- the engine run ids this purge is about, as `["<uuid>", ...]`
 *   base_folder_name <- request.id (the TWIST request)
 *
 * A purge appears in the same `run_history` as every other engine, under `module_name = 'purge'`.
 * That is deliberate: "what ran against this cluster" should be one question with one answer, and a
 * deletion is the last thing that should be recorded somewhere of its own.
 */
object PurgeAudit {

  final val MODULE_NAME = "purge"

  final val AUDIT = "audit"

  /**
   * Start the run_history audit for a purge run.
   *
   * @param config   the parsed application config (root holds the `purge_app { }` block)
   * @param usedConf path of the application.conf used, recorded as `used_conf`
   * @param runs     the engine runs being purged, named in `scenarios` so the audit row says WHAT
   *                 was purged and not merely that a purge happened
   */
  def start(config: Config, usedConf: String, runs: Seq[EngineRun] = Seq.empty)
           (implicit spark: SparkSession): RunAudit = {
    val request = block(config, PrimaryConstants.REQUEST)

    RunAudit.start(
      moduleName = MODULE_NAME,
      auditConfig = block(config, AUDIT),
      usedConf = usedConf,
      userLauncher = Some(PrimaryUtilities.getStringOr(request, "requester", "")).filter(_.nonEmpty),
      motor = Some(MODULE_NAME),
      projectionDates = Some(PrimaryRunner.asOfDate(config).toString),
      scenarios =
        if (runs.isEmpty) None
        else Some(runs.map(run => "\"" + run.runId + "\"").mkString("[", ", ", "]")),
      baseFolderName = Some(PrimaryRunner.requestId(config)).filter(_.nonEmpty))
  }

  private def block(config: Config, name: String): Config = {
    val path = s"${PrimaryConstants.APP_CONF}.$name"
    if (config.hasPath(path)) config.getConfig(path) else ConfigFactory.empty()
  }
}
