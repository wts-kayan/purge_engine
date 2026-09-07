package com.bnp.str.utilities.audit

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.util.Collections

/**
 * Storage layer for the `run_history` audit: ORC files laid out as Hive-style partitions
 * `module_name=…/run_id=…` under a fixed location, exposed as an EXTERNAL Hive table
 * PARTITIONED BY (module_name, run_id). One partition per run.
 *
 * Two concerns are deliberately split:
 *  1. DATA WRITE — Spark's ORC datasource with `partitionBy("module_name","run_id")` and dynamic
 *     partition overwrite. Needs NO metastore, so it works the same locally and on the cluster, and
 *     the row written at start (end_date/duration null) is replaced in place by the final row.
 *  2. TABLE REGISTRATION — `CREATE EXTERNAL TABLE IF NOT EXISTS … STORED AS ORC` + `ADD PARTITION`.
 *     BEST-EFFORT and guarded: if the metastore is unavailable (e.g. a local box without Hadoop
 *     native IO), the ORC data is still written; only the catalog entry is skipped with a warning.
 */
object RunAuditStore {

  private val log = LoggerFactory.getLogger(getClass)

  final val DEFAULT_TABLE = "run_history"

  /** Non-partition (data) columns, in table order. */
  val dataSchema: StructType = StructType(Seq(
    StructField("application_id",   StringType),
    StructField("used_jar",         StringType),
    StructField("used_conf",        StringType),
    StructField("user_launcher",    StringType),
    StructField("status",           StringType),
    StructField("creation_date",    TimestampType),
    StructField("end_date",         TimestampType),
    StructField("duration",         StringType),
    StructField("motor",            StringType),
    StructField("projection_dates", StringType),
    StructField("scenarios",        StringType),
    StructField("base_folder_name", StringType)
  ))

  /** Full write schema: data columns first, then the partition columns (module_name, run_id) LAST. */
  val schema: StructType = dataSchema.add("module_name", StringType).add("run_id", StringType)

  private def sqlType(dt: DataType): String = dt match {
    case TimestampType => "TIMESTAMP"
    case LongType      => "BIGINT"
    case _             => "STRING"
  }

  private def ddlColumns: String =
    dataSchema.fields.map(f => s"`${f.name}` ${sqlType(f.dataType)}").mkString(",\n  ")

  /** DDL that registers the EXTERNAL ORC table (partitioned by module_name, run_id) at `location`. */
  def createTableDDL(table: String, location: String): String =
    s"""CREATE EXTERNAL TABLE IF NOT EXISTS $table (
       |  $ddlColumns
       |)
       |PARTITIONED BY (`module_name` STRING, `run_id` STRING)
       |STORED AS ORC
       |LOCATION '$location'""".stripMargin

  /** DDL that pins the table's data so DROP TABLE keeps the ORC files (external table, no purge). */
  def alterProps(fullTableName: String): String =
    "ALTER TABLE " + fullTableName + "\n" +
      " SET TBLPROPERTIES ('external.table.purge'='FALSE')"

  /** Write (or overwrite) this run's ORC partition, then best-effort register it in the metastore. */
  def write(record: RunAuditRecord, table: String, location: String)(implicit spark: SparkSession): Unit = {
    val row = Row(
      record.applicationId, record.usedJar, record.usedConf, record.userLauncher, record.status,
      record.creationDate, record.endDate.orNull, record.duration.orNull, record.motor,
      record.projectionDates.orNull, record.scenarios.orNull, record.baseFolderName.orNull,
      record.moduleName, record.runId
    )

    val df = spark.createDataFrame(Collections.singletonList(row), schema)
    // ORC datasource write; dynamic overwrite -> only this (module_name, run_id) partition is replaced.
    df.write.mode("overwrite").format("orc").partitionBy("module_name", "run_id").save(location)

    registerPartition(table, location, record.moduleName, record.runId)
  }

  /** Best-effort: ensure the external table exists and this run's partition is registered. */
  private def registerPartition(table: String, location: String, moduleName: String, runId: String)
                               (implicit spark: SparkSession): Unit =
    try {
      spark.sql(createTableDDL(table, location))
      spark.sql(alterProps(table))
      spark.sql(s"ALTER TABLE $table ADD IF NOT EXISTS PARTITION (`module_name`='$moduleName', `run_id`='$runId')")
    } catch {
      case e: Throwable =>
        log.warn(s"[audit] ORC written to $location but external table '$table' registration skipped " +
          s"(no metastore?): ${e.getClass.getSimpleName}: ${e.getMessage}")
    }

  /** Read the full run_history via the catalog (all modules, all runs). Requires the table to exist. */
  def read(table: String = DEFAULT_TABLE)(implicit spark: SparkSession): DataFrame =
    spark.table(table)

  /** Read the run_history directly from ORC files at `location` (no metastore needed). */
  def readFiles(location: String)(implicit spark: SparkSession): DataFrame =
    spark.read.schema(schema).orc(location)
}
