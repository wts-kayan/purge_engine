package com.bnp.str.purge.audit

import com.bnp.str.purge.purge.PurgeDetailRecord
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Storage for `purge_detail`: the object-level twin of `run_history`, and the record that answers
 * "what exactly did that purge remove?" long after everyone has forgotten.
 *
 * Same two-part shape as [[com.bnp.str.utilities.audit.RunAuditStore]], for the same reasons:
 *
 *  1. **DATA WRITE** through Spark's ORC datasource, partitioned by `(purge_date, run_id)` with
 *     dynamic overwrite. Needs no metastore, so it behaves identically on a laptop and on the
 *     cluster, and re-running a run replaces its own partition rather than appending a second copy.
 *  2. **TABLE REGISTRATION** best-effort. If the metastore is unavailable the ORC is still written
 *     and only the catalogue entry is skipped, with a warning — losing the audit because Hive was
 *     down would be worse than losing the ability to query it with SQL today.
 *
 * The one difference from `run_history`: this table is written AFTER data has been deleted. A
 * failure to write it does not undo the deletion, so the caller logs loudly and the ORC path is the
 * fallback record.
 *
 * `source_engine` / `source_run_id` say which engine run each object belonged to. Without them the
 * question "what happened to run X?" is only answerable at RUN level, through
 * `run_history.scenarios` — and for a table-granular engine, whose rows carry no partition spec and
 * whose table names carry no run id, it is not answerable per object at all.
 */
object PurgeDetailStore {

  private val log = LoggerFactory.getLogger(getClass)

  final val DEFAULT_TABLE = "purge_detail"

  /** Non-partition (data) columns, in table order. */
  val dataSchema: StructType = StructType(Seq(
    StructField("request_id", StringType),
    StructField("path", StringType),
    StructField("database_name", StringType),
    StructField("table_name", StringType),
    StructField("partition_spec", StringType),
    StructField("strategy", StringType),
    StructField("status", StringType),
    StructField("bytes_freed", LongType),
    StructField("num_files", LongType),
    StructField("trash_path", StringType),
    StructField("restore_deadline", TimestampType),
    StructField("error_message", StringType),
    StructField("executed_at", TimestampType),
    StructField("user_launcher", StringType),
    // Appended rather than slotted in beside request_id: a table already registered from an earlier
    // version keeps its column order, and appending is the one change an ALTER TABLE ADD COLUMNS
    // can make to it without rewriting anything.
    StructField("source_engine", StringType),
    StructField("source_run_id", StringType)
  ))

  /** Full write schema: data columns first, then the partition columns LAST. */
  val schema: StructType = dataSchema.add("purge_date", StringType).add("run_id", StringType)

  private def sqlType(dt: DataType): String = dt match {
    case TimestampType => "TIMESTAMP"
    case LongType => "BIGINT"
    case _ => "STRING"
  }

  private def ddlColumns: String =
    dataSchema.fields.map(f => s"`${f.name}` ${sqlType(f.dataType)}").mkString(",\n  ")

  /** DDL registering the EXTERNAL ORC table, partitioned by (purge_date, run_id), at `location`. */
  def createTableDDL(table: String, location: String): String =
    s"""CREATE EXTERNAL TABLE IF NOT EXISTS $table (
       |  $ddlColumns
       |)
       |PARTITIONED BY (`purge_date` STRING, `run_id` STRING)
       |STORED AS ORC
       |LOCATION '$location'""".stripMargin

  /**
   * Pin the data so DROP TABLE keeps the ORC files.
   *
   * Every other STR table is `external.table.purge = TRUE` (open question Q8), which means dropping
   * one deletes its data. That is the opposite of what an audit needs: the record of a deletion must
   * outlive any accident involving the table that holds it.
   */
  def alterProps(fullTableName: String): String =
    s"ALTER TABLE $fullTableName SET TBLPROPERTIES ('external.table.purge'='FALSE')"

  def toDataFrame(records: Seq[PurgeDetailRecord], purgeDate: String)
                 (implicit spark: SparkSession): DataFrame = {
    val rows = records.map(r => Row(
      r.requestId, r.path, r.databaseName, r.tableName, r.partitionSpec, r.strategy, r.status,
      r.bytesFreed, r.numFiles, r.trashPath, r.restoreDeadline.orNull, r.errorMessage,
      r.executedAt, r.userLauncher, r.sourceEngine, r.sourceRunId, purgeDate, r.runId))
    spark.createDataFrame(rows.asJava, schema)
  }

  /** Write this run's ORC partition, then best-effort register it in the metastore. */
  def write(records: Seq[PurgeDetailRecord], purgeDate: String, runId: String,
            table: String, location: String)(implicit spark: SparkSession): Unit = {
    toDataFrame(records, purgeDate)
      .write.mode("overwrite").format("orc").partitionBy("purge_date", "run_id").save(location)

    registerPartition(table, location, purgeDate, runId)
  }

  private def registerPartition(table: String, location: String, purgeDate: String, runId: String)
                               (implicit spark: SparkSession): Unit =
    try {
      spark.sql(createTableDDL(table, location))
      spark.sql(alterProps(table))
      spark.sql(s"ALTER TABLE $table ADD IF NOT EXISTS PARTITION " +
        s"(`purge_date`='$purgeDate', `run_id`='$runId')")
    } catch {
      case e: Throwable =>
        log.warn(s"[audit] ORC written to $location but external table '$table' registration " +
          s"skipped (no metastore?): ${e.getClass.getSimpleName}: ${e.getMessage}")
    }

  /** Read the detail directly from ORC files (no metastore needed) — used by a restore. */
  def readFiles(location: String)(implicit spark: SparkSession): DataFrame =
    spark.read.schema(schema).orc(location)
}
