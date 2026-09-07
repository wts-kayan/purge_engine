package com.bnp.str.purge.common

import com.bnp.str.purge.control.{CheckConfig, PurgeControlMapper}
import com.bnp.str.purge.mapping.{ManifestView, PrimaryMapper}
import com.bnp.str.purge.reader.PrimaryReader
import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.time.LocalDate

/**
 * Orchestrates a simulation: inventory and referentials in, manifest out.
 *
 * The candidate set is cached before the fingerprint is taken, and the SAME cached frame is what
 * gets written. Without that, Spark would recompute the selection — re-walking HDFS — and could
 * fingerprint one view of the filesystem while writing another. A manifest whose fingerprint does
 * not describe its own rows would make PC12 meaningless at execution time.
 */
class PrimaryRunner(primaryReader: PrimaryReader, outputTableName: String)
                   (implicit sparkSession: SparkSession, conf: Config)
  extends RunnerProvider(primaryReader) {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def run_purge_runner(runId: String): PurgeOutcome = {

    log.info(s"run_purge_runner runId=$runId (${this.getClass.getName})")

    val today = PrimaryRunner.asOfDate(conf)
    val requestId = PrimaryRunner.requestId(conf)

    val candidates = new PrimaryMapper(
      hdfs_inventory,
      hive_catalog,
      purge_policy,
      purge_scope,
      outputTableName,
      today
    ).getMapping_purge.cache()

    // Fingerprinted BEFORE the controls run, and over the candidate set alone: what an approval
    // covers is which objects are in play and what state they were in, not the verdicts the rules
    // reached about them. Re-running the controls with a stricter conf on the same filesystem must
    // produce the same fingerprint, or PC12 would report drift where nothing moved.
    val fingerprint = ManifestView.fingerprint(candidates)
    log.info(s"Manifest fingerprint: $fingerprint")

    val manifest = ManifestView.build(candidates, runId, requestId, today, fingerprint)

    val checks = CheckConfig.from(conf)
    val outcome = new PurgeControlMapper(checks, run_history, primaryReader.protectedPaths())
      .apply(manifest, PrimaryRunner.source(conf), runId, requestId, fingerprint)

    PurgeOutcome(outcome.manifest, outcome.report, fingerprint)
  }
}

object PrimaryRunner {

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * The reference date every retention is measured against: `request.asOfDate` when TWIST pins one,
   * the system date otherwise.
   *
   * Pinning it matters more than it looks. A simulation run late on the last day of a quarter and
   * approved the next morning must judge the same objects both times; if the engine read the clock,
   * the execution could legitimately compute a different eligible set from the one that was
   * approved — and PC12 would catch it as drift, which is the right outcome but a confusing one.
   */
  def asOfDate(conf: Config): LocalDate = {
    val path = s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.REQUEST}"
    val raw =
      if (conf.hasPath(path)) PrimaryUtilities.getStringOr(conf.getConfig(path), "asOfDate", "") else ""

    if (raw.isEmpty) LocalDate.now()
    else
      try LocalDate.parse(raw)
      catch {
        case _: Throwable =>
          throw new IllegalArgumentException(
            s"$path.asOfDate = '$raw' is not an ISO date (yyyy-MM-dd)")
      }
  }

  /** What this run inspected, for the report header: the scan roots, as configured. */
  def source(conf: Config): String = {
    val scan = conf.getConfig(s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.SCAN}")
    val roots = PrimaryUtilities.getStringList(scan, PrimaryConstants.SCAN_ROOTS)
    val databases = PrimaryUtilities.getStringList(scan, PrimaryConstants.SCAN_DATABASES)
    (roots ++ databases.map(db => s"hive:$db")).mkString(", ")
  }

  /** The TWIST request this run belongs to; empty for a run launched by hand. */
  def requestId(conf: Config): String = {
    val path = s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.REQUEST}"
    val id = if (conf.hasPath(path)) PrimaryUtilities.getStringOr(conf.getConfig(path), "id", "") else ""
    if (id.isEmpty) log.warn(s"$path.id is not set; the manifest will not be tied to a TWIST request")
    id
  }
}
