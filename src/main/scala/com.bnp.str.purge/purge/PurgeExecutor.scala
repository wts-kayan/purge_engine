package com.bnp.str.purge.purge

import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, Trash}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import java.sql.Timestamp

/**
 * Performs the deletion.
 *
 * This is the only class in the engine that removes anything, and everything about its shape is
 * chosen so that a run can be explained afterwards:
 *
 *  - **It reads the manifest and nothing else.** It never re-derives what to delete, never re-scans,
 *    never consults a policy. What was simulated is what is executed.
 *  - **It acts only on DELETE and DELETE_WITH_WARNING.** Every other row is recorded as skipped, so
 *    `purge_detail` is a complete account of the manifest rather than a list of successes.
 *  - **It runs on the driver.** Deleting is a metadata call to the NameNode; fanning it across
 *    executors buys nothing, makes ordering unobservable, and turns one failed object into a failed
 *    task, a retry, and a second attempt at a delete that already happened.
 *  - **One object's failure is that object's failure.** The run continues and ends PARTIAL. A purge
 *    that aborts halfway leaves the operator with no record of where it stopped.
 *  - **It is idempotent.** A path already gone is `SKIPPED_ABSENT`, not an error, so re-running a
 *    manifest is safe — which is what makes recovering from a PARTIAL run possible at all.
 */
class PurgeExecutor()(implicit sparkSession: SparkSession, conf: Config) {

  import PurgeExecutor._

  private val log = LoggerFactory.getLogger(this.getClass)

  private val executionConfig: Config = {
    val path = s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.EXECUTION}"
    if (conf.hasPath(path)) conf.getConfig(path) else ConfigFactory.empty()
  }

  private val defaultStrategy: PurgeStrategy =
    PurgeStrategy.of(PrimaryUtilities.getStringOr(executionConfig, "defaultStrategy",
      PrimaryConstants.STRATEGY_TRASH), PurgeStrategy.Trash)

  private val dropHivePartition: Boolean =
    PrimaryUtilities.getBooleanOr(executionConfig, "dropHivePartition", default = true)

  /**
   * Whether a purged object that IS a whole registered table also has its table dropped.
   *
   * On by default, for the same reason `dropHivePartition` is: leaving the table registered against
   * a location whose data has just been trashed is orphan metadata — a table every consumer can
   * still see and nothing can read. It is separately switchable because dropping a table is a larger
   * act than dropping a partition, and a perimeter may want the data gone and the definition kept.
   */
  private val dropHiveTable: Boolean =
    PrimaryUtilities.getBooleanOr(executionConfig, "dropHiveTable", default = true)

  private val stopOnFirstError: Boolean =
    PrimaryUtilities.getBooleanOr(executionConfig, "stopOnFirstError", default = false)

  private val logEveryNObjects: Int =
    PrimaryUtilities.getIntOr(executionConfig, "logEveryNObjects", 100)

  /**
   * True when a TRASH deletion must FAIL if Trash is unavailable, rather than quietly doing nothing.
   *
   * Hadoop's `moveToAppropriateTrash` returns false when `fs.trash.interval` is 0, leaving the data
   * in place. Treating that as success would report a purge that did not happen; falling back to a
   * hard delete would silently destroy the recoverability the strategy was chosen for. Neither is
   * acceptable, so the object fails and says why.
   */
  private val failWhenTrashUnavailable: Boolean =
    PrimaryUtilities.getBooleanOr(executionConfig, "failWhenTrashUnavailable", default = true)

  def execute(manifest: DataFrame, runId: String, userLauncher: String): PurgeResult = {
    val rows = manifest.select(MANIFEST_COLUMNS.map(col): _*).collect()
    val executedAt = new Timestamp(System.currentTimeMillis())
    val restoreDeadline = trashRestoreDeadline(executedAt)

    val deletable = rows.count(row => isDeletable(decisionOf(row)))
    log.info(s"Executing purge run $runId: ${rows.length} manifest row(s), $deletable to delete, " +
      s"default strategy ${defaultStrategy.name}" +
      restoreDeadline.map(d => s", restorable until $d").getOrElse(", NO trash window"))

    var stopped = false
    val records = rows.toList.zipWithIndex.map { case (row, index) =>
      val record =
        if (stopped) skipped(row, runId, userLauncher, executedAt, STATUS_SKIPPED_ABORTED,
          "the run stopped after an earlier failure")
        else if (!isDeletable(decisionOf(row)))
          skipped(row, runId, userLauncher, executedAt, STATUS_SKIPPED_BLOCKED,
            s"decision is ${decisionOf(row)}")
        else purgeOne(row, runId, userLauncher, executedAt, restoreDeadline)

      if (record.status == STATUS_FAILED && stopOnFirstError) {
        log.error(s"execution.stopOnFirstError is on and ${record.path} failed; stopping the run")
        stopped = true
      }
      if ((index + 1) % logEveryNObjects == 0)
        log.info(s"[purge] ${index + 1}/${rows.length} object(s) processed")
      record
    }

    val result = PurgeResult(runId, records)
    log.info(result.summaryLine)
    result
  }

  // ---------------------------------------------------------------------------------------------

  /** Remove one object, or record precisely why it was not removed. */
  private def purgeOne(row: Row,
                       runId: String,
                       userLauncher: String,
                       executedAt: Timestamp,
                       restoreDeadline: Option[Timestamp]): PurgeDetailRecord = {

    val path = new Path(stringOf(row, "path"))
    val strategy =
      try PurgeStrategy.of(stringOf(row, "strategy"), defaultStrategy)
      catch {
        case e: IllegalArgumentException =>
          return failure(row, runId, userLauncher, executedAt, e.getMessage)
      }

    try {
      val fs = path.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
      val status = Option(if (fs.exists(path)) fs.getFileStatus(path) else null)

      status match {
        // PC13, at the moment it actually means something: gone since the simulation.
        case None =>
          record(row, runId, userLauncher, executedAt, strategy, STATUS_SKIPPED_ABSENT,
            bytesFreed = 0L, trashPath = "", restoreDeadline = None,
            error = "the path no longer exists")

        // PC12 per object: the same path, holding something else than what was simulated.
        case Some(current) if drifted(fs, row, current) =>
          record(row, runId, userLauncher, executedAt, strategy, STATUS_SKIPPED_DRIFT,
            bytesFreed = 0L, trashPath = "", restoreDeadline = None,
            error = driftReason(fs, row, current))

        case Some(_) => apply(row, runId, userLauncher, executedAt, strategy, fs, path, restoreDeadline)
      }
    } catch {
      case e: Throwable =>
        log.error(s"[purge] FAILED ${path.toString}: ${e.getClass.getSimpleName}: ${e.getMessage}")
        failure(row, runId, userLauncher, executedAt, s"${e.getClass.getSimpleName}: ${e.getMessage}")
    }
  }

  /** Carry out the strategy, then reconcile the metastore when the object was a registered partition. */
  private def apply(row: Row,
                    runId: String,
                    userLauncher: String,
                    executedAt: Timestamp,
                    strategy: PurgeStrategy,
                    fs: FileSystem,
                    path: Path,
                    restoreDeadline: Option[Timestamp]): PurgeDetailRecord = {

    val bytes = longOf(row, "size_bytes")
    val registered = booleanOf(row, "is_registered_partition")
    val registeredTable = booleanOf(row, "is_registered_table")

    strategy match {
      case PurgeStrategy.Logical =>
        record(row, runId, userLauncher, executedAt, strategy, STATUS_LOGICALLY_DELETED,
          bytesFreed = 0L, trashPath = "", restoreDeadline = None,
          error = "recorded as purged; the data was left in place")

      case PurgeStrategy.Hard =>
        fs.delete(path, true)
        if (!maybeDropPartition(row, registered)) maybeDropTable(row, registeredTable)
        record(row, runId, userLauncher, executedAt, strategy, STATUS_DELETED,
          bytesFreed = bytes, trashPath = "", restoreDeadline = None, error = "")

      // TRASH and DROP_PARTITION take the same path on purpose: the data moves to Trash FIRST, and
      // Hive is only ever asked to drop metadata whose location is already empty. That is what keeps
      // the 7-day window true for a partition of a table with external.table.purge = TRUE, where a
      // bare DROP PARTITION would have deleted the data through Hive instead.
      case PurgeStrategy.Trash | PurgeStrategy.DropPartition =>
        val moved = Trash.moveToAppropriateTrash(fs, path, sparkSession.sparkContext.hadoopConfiguration)
        if (!moved && failWhenTrashUnavailable)
          failure(row, runId, userLauncher, executedAt,
            "Trash is unavailable (fs.trash.interval = 0?); refusing to fall back to a hard delete")
        else {
          val partitionDropped = maybeDropPartition(row, registered)
          val tableDropped = !partitionDropped && maybeDropTable(row, registeredTable)
          val status =
            if (partitionDropped) STATUS_PARTITION_DROPPED
            else if (tableDropped) STATUS_TABLE_DROPPED
            else if (moved) STATUS_TRASHED
            else STATUS_DELETED
          record(row, runId, userLauncher, executedAt, strategy, status,
            bytesFreed = bytes, trashPath = if (moved) trashLocation(fs, path) else "",
            restoreDeadline = if (moved) restoreDeadline else None, error = "")
        }
    }
  }

  /**
   * Drop the partition the object was, once its data is gone. Best-effort and reported: leaving
   * orphan metadata behind is a nuisance, but failing the object AFTER its data has been removed
   * would make the run look worse than it is and invite a re-run that has nothing left to do.
   */
  private def maybeDropPartition(row: Row, registered: Boolean): Boolean = {
    if (!registered || !dropHivePartition) return false

    val database = stringOf(row, "database_name")
    val table = stringOf(row, "table_name")
    val spec = stringOf(row, "partition_spec")
    if (database.isEmpty || table.isEmpty || spec.isEmpty) return false

    val sql = s"ALTER TABLE `$database`.`$table` DROP IF EXISTS PARTITION (${partitionPredicate(spec)})"
    try {
      sparkSession.sql(sql)
      log.info(s"[purge] dropped partition $database.$table ($spec)")
      true
    } catch {
      case e: Throwable =>
        log.warn(s"[purge] data removed but the partition could not be dropped — $sql " +
          s"(${e.getClass.getSimpleName}: ${e.getMessage})")
        false
    }
  }

  /**
   * Drop the TABLE the object was, once its data is gone.
   *
   * The table-granular twin of [[maybeDropPartition]], and it exists for the same reason: for a
   * table-granular engine — the classic simulator — a run IS its tables, so removing the data and
   * leaving the tables registered would be a half-done purge that still reports success.
   *
   * The ordering is what makes it safe, and it is not optional. Every STR output table is EXTERNAL
   * with `external.table.purge = TRUE` — confirmed for projection AND simulator — so a bare
   * `DROP TABLE` deletes the data itself, through Hive, outside this engine's deletion path and with
   * no certainty that Trash is honoured. By the time this runs the data has already been moved to
   * Trash and the location is empty, so the drop is pure metadata and the 7-day restore window
   * still holds.
   *
   * Best-effort and reported, like the partition drop: failing the object AFTER its data is gone
   * would make the run look worse than it is and invite a re-run with nothing left to do.
   */
  private def maybeDropTable(row: Row, registeredTable: Boolean): Boolean = {
    if (!registeredTable || !dropHiveTable) return false

    val database = stringOf(row, "database_name")
    val table = stringOf(row, "table_name")
    if (database.isEmpty || table.isEmpty) return false

    // A row carrying a partition spec is a partition of a table, never the table itself. Refusing
    // here as well as at the call site is deliberate: of all the mistakes this engine could make,
    // dropping a table where a partition was meant is the one it must not make twice.
    if (stringOf(row, "partition_spec").nonEmpty) {
      log.warn(s"[purge] $database.$table is marked as a whole table but carries a partition spec; " +
        "refusing to drop the table")
      return false
    }

    val sql = s"DROP TABLE IF EXISTS `$database`.`$table`"
    try {
      sparkSession.sql(sql)
      log.info(s"[purge] dropped table $database.$table")
      true
    } catch {
      case e: Throwable =>
        log.warn(s"[purge] data removed but the table could not be dropped - $sql " +
          s"(${e.getClass.getSimpleName}: ${e.getMessage})")
        false
    }
  }

  /**
   * The moment after which Trash no longer holds the data: now plus `fs.trash.interval`, which on
   * Promethee is 7 days. None when Trash is disabled, and the report then promises no window at all.
   */
  private def trashRestoreDeadline(executedAt: Timestamp): Option[Timestamp] = {
    val minutes = sparkSession.sparkContext.hadoopConfiguration.getLong("fs.trash.interval", 0L)
    if (minutes <= 0L) None
    else Some(new Timestamp(executedAt.getTime + minutes * 60L * 1000L))
  }

  /**
   * Where Trash put it, by the standard `<home>/.Trash/Current<path>` convention.
   *
   * Hadoop does not tell the caller where it moved the file, and the convention is what every
   * restore procedure already relies on. Recorded so a restore has somewhere to look, not so the
   * engine can trust it blindly.
   */
  private def trashLocation(fs: FileSystem, path: Path): String =
    PrimaryUtilities.normalizePath(
      new Path(fs.getHomeDirectory, s".Trash/Current${PrimaryUtilities.normalizePath(path.toString)}").toString)

  /**
   * Has this object changed since the simulation recorded it?
   *
   * The comparison has to use the SAME definition of size and modification time that the inventory
   * used, or it reports drift where nothing moved. For a leaf directory — the usual purge unit — the
   * inventory records the summed size of the files it holds and the NEWEST timestamp among the
   * directory and those files. Comparing that against the directory's own `getLen` (0, or a block
   * size) and its own mtime (older than a file written into it a moment later) makes almost every
   * partition look drifted, and a false drift is not a harmless caution: the object is skipped, the
   * run reports SUCCESS, and the purge quietly did not happen.
   */
  private def drifted(fs: FileSystem, row: Row, current: FileStatus): Boolean = {
    val (size, mtime) = currentState(fs, current)
    val expectedSize = longOf(row, "size_bytes")
    val expectedMtime = Option(row.getAs[Timestamp]("modification_time")).map(_.getTime).getOrElse(0L)
    size != expectedSize || (expectedMtime > 0L && mtime != expectedMtime)
  }

  /** Size and modification time as [[com.bnp.str.purge.reader.InventoryReader]] would report them. */
  private def currentState(fs: FileSystem, status: FileStatus): (Long, Long) =
    if (!status.isDirectory) (status.getLen, status.getModificationTime)
    else {
      val children = try fs.listStatus(status.getPath) catch { case _: Throwable => Array.empty[FileStatus] }
      val files = children.filter(!_.isDirectory)
      (files.map(_.getLen).sum, (status.getModificationTime +: files.map(_.getModificationTime)).max)
    }

  private def driftReason(fs: FileSystem, row: Row, current: FileStatus): String = {
    val (size, mtime) = currentState(fs, current)
    s"changed since the simulation: size ${longOf(row, "size_bytes")} -> $size, " +
      s"modified ${row.getAs[Timestamp]("modification_time")} -> ${new Timestamp(mtime)}"
  }

  private def record(row: Row,
                     runId: String,
                     userLauncher: String,
                     executedAt: Timestamp,
                     strategy: PurgeStrategy,
                     status: String,
                     bytesFreed: Long,
                     trashPath: String,
                     restoreDeadline: Option[Timestamp],
                     error: String): PurgeDetailRecord =
    PurgeDetailRecord(
      runId = runId,
      requestId = stringOf(row, "request_id"),
      // Which engine run this object belonged to, carried from the scope through the manifest so
      // that `purge_detail` can be filtered by it directly. A table-granular row has no partition
      // spec to recover it from.
      sourceEngine = stringOf(row, "source_engine"),
      sourceRunId = stringOf(row, "source_run_id"),
      path = stringOf(row, "path"),
      databaseName = stringOf(row, "database_name"),
      tableName = stringOf(row, "table_name"),
      partitionSpec = stringOf(row, "partition_spec"),
      strategy = strategy.name,
      status = status,
      bytesFreed = bytesFreed,
      numFiles = longOf(row, "num_files"),
      trashPath = trashPath,
      restoreDeadline = restoreDeadline,
      errorMessage = error,
      executedAt = executedAt,
      userLauncher = userLauncher)

  private def skipped(row: Row, runId: String, user: String, at: Timestamp,
                      status: String, why: String): PurgeDetailRecord =
    record(row, runId, user, at, defaultStrategy, status, 0L, "", None, why)

  private def failure(row: Row, runId: String, user: String, at: Timestamp, why: String): PurgeDetailRecord =
    record(row, runId, user, at, defaultStrategy, STATUS_FAILED, 0L, "", None, why)

  private def decisionOf(row: Row): String = stringOf(row, "decision")

  private def isDeletable(decision: String): Boolean =
    PrimaryConstants.DELETABLE_DECISIONS.contains(decision)
}

object PurgeExecutor {

  /** The manifest columns the executor reads. Anything else in the manifest is not its business. */
  val MANIFEST_COLUMNS = Seq("path", "request_id", "database_name", "table_name", "partition_spec",
    "is_registered_partition", "is_registered_table", "strategy", "decision", "size_bytes",
    "num_files", "modification_time", "source_engine", "source_run_id")

  val STATUS_DELETED = "DELETED"
  val STATUS_TRASHED = "TRASHED"
  val STATUS_PARTITION_DROPPED = "PARTITION_DROPPED"
  val STATUS_TABLE_DROPPED = "TABLE_DROPPED"
  val STATUS_LOGICALLY_DELETED = "LOGICALLY_DELETED"
  val STATUS_SKIPPED_BLOCKED = "SKIPPED_BLOCKED"
  val STATUS_SKIPPED_ABSENT = "SKIPPED_ABSENT"
  val STATUS_SKIPPED_DRIFT = "SKIPPED_DRIFT"
  val STATUS_SKIPPED_ABORTED = "SKIPPED_ABORTED"
  val STATUS_FAILED = "FAILED"

  /** Statuses meaning the object is gone from where it was. */
  val REMOVED_STATUSES =
    Set(STATUS_DELETED, STATUS_TRASHED, STATUS_PARTITION_DROPPED, STATUS_TABLE_DROPPED)

  /**
   * `runid=abc/scenario=FW` -> ``` `runid`='abc', `scenario`='FW' ```
   *
   * Values reach here from a filesystem path, so a quote in one is escaped rather than trusted; a
   * partition value is not a place to find out that string concatenation builds SQL.
   */
  private[str] def partitionPredicate(spec: String): String =
    spec.split("/").filter(_.nonEmpty).map { segment =>
      val cut = segment.indexOf('=')
      val key = segment.substring(0, cut)
      val value = segment.substring(cut + 1).replace("'", "\\'")
      s"`$key`='$value'"
    }.mkString(", ")

  private def stringOf(row: Row, name: String): String =
    if (row.schema.fieldNames.contains(name)) Option(row.getAs[Any](name)).map(_.toString).getOrElse("") else ""

  private def longOf(row: Row, name: String): Long =
    if (row.schema.fieldNames.contains(name)) Option(row.getAs[Any](name)).map(_.toString.toLong).getOrElse(0L) else 0L

  private def booleanOf(row: Row, name: String): Boolean =
    row.schema.fieldNames.contains(name) && Option(row.getAs[Any](name)).exists(_.toString == "true")
}

/** One object, and what the execution did to it. Persisted as a row of `purge_detail`. */
final case class PurgeDetailRecord(runId: String,
                                   requestId: String,
                                   sourceEngine: String,
                                   sourceRunId: String,
                                   path: String,
                                   databaseName: String,
                                   tableName: String,
                                   partitionSpec: String,
                                   strategy: String,
                                   status: String,
                                   bytesFreed: Long,
                                   numFiles: Long,
                                   trashPath: String,
                                   restoreDeadline: Option[Timestamp],
                                   errorMessage: String,
                                   executedAt: Timestamp,
                                   userLauncher: String)

/** What one execution did, in full. */
final case class PurgeResult(runId: String, records: Seq[PurgeDetailRecord]) {

  import PurgeExecutor._

  def removed: Seq[PurgeDetailRecord] = records.filter(r => REMOVED_STATUSES.contains(r.status))

  def failed: Seq[PurgeDetailRecord] = records.filter(_.status == STATUS_FAILED)

  def bytesFreed: Long = removed.map(_.bytesFreed).sum

  /** SUCCESS only when nothing failed; a partial purge must never read as a clean one. */
  def status: String = if (failed.isEmpty) "SUCCESS" else "PARTIAL"

  def summaryLine: String = {
    val byStatus = records.groupBy(_.status).map { case (s, rs) => s"$s=${rs.size}" }.toSeq.sorted
    s"PURGE $status - ${removed.size} object(s) removed, " +
      s"${com.bnp.str.purge.utility.PrimaryUtilities.humanBytes(bytesFreed)} freed" +
      (if (failed.isEmpty) "" else s", ${failed.size} FAILED") +
      s" [${byStatus.mkString(", ")}]"
  }
}
