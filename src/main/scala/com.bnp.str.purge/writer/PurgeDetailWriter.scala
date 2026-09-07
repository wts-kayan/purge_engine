package com.bnp.str.purge.writer

import com.bnp.str.purge.audit.PurgeDetailStore
import com.bnp.str.purge.purge.PurgeResult
import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Writes what the execution did, object by object.
 *
 * Guarded, and deliberately so: this write happens AFTER data has been deleted, and throwing here
 * would fail a run whose destructive part already succeeded — inviting a re-run that finds nothing
 * left and reports a purge which removed nothing. So a failure is logged at ERROR with the whole
 * result inline, and the account survives in the driver log even when the table could not be
 * written. That is the difference between losing the query and losing the record.
 */
object PurgeDetailWriter {

  private val log = LoggerFactory.getLogger(this.getClass)

  def write(result: PurgeResult, purgeDate: String)(implicit spark: SparkSession, conf: Config): Unit = {
    val path = s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.PURGE_DETAIL}"

    if (!conf.hasPath(path)) {
      log.warn(s"$path is not configured; the per-object record of this purge is in this log only")
      logResult(result)
      return
    }

    val detailConfig = conf.getConfig(path)
    if (!PrimaryUtilities.getBooleanOr(detailConfig, "enable", default = true)) {
      log.warn(s"$path.enable = false; the per-object record of this purge is in this log only")
      logResult(result)
      return
    }

    val database = PrimaryUtilities.getStringOr(detailConfig, "database", "")
    val tableName = PrimaryUtilities.getStringOr(detailConfig, "table", PurgeDetailStore.DEFAULT_TABLE)
    val table = if (database.isEmpty) tableName else s"$database.$tableName"
    val location = PrimaryUtilities.qualifyPath(
      PrimaryUtilities.getStringOr(detailConfig, "root", PurgeDetailStore.DEFAULT_TABLE))

    try {
      PurgeDetailStore.write(result.records, purgeDate, result.runId, table, location)
      log.info(s"purge_detail written: ${result.records.size} row(s) -> $location " +
        s"(purge_date=$purgeDate, run_id=${result.runId})")
    } catch {
      case e: Throwable =>
        log.error(s"COULD NOT WRITE purge_detail to $location " +
          s"(${e.getClass.getSimpleName}: ${e.getMessage}). The deletion HAS happened; the only " +
          "record of it is the lines that follow.", e)
        logResult(result)
    }
  }

  /** The whole result, one line per object, so the log is a usable fallback record. */
  private def logResult(result: PurgeResult): Unit = {
    log.warn(result.summaryLine)
    result.records.foreach(r =>
      log.warn(s"[purge_detail] ${r.status} ${r.path} strategy=${r.strategy} " +
        s"bytes=${r.bytesFreed} trash=${r.trashPath} ${r.errorMessage}"))
  }
}
