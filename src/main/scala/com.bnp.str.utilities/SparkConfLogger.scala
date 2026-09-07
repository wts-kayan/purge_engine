package com.bnp.str.utilities

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Logs the effective Spark configuration once, right after the session is built.
 *
 * On a cluster the settings that actually apply are the merge of spark-defaults, the launcher's
 * `--conf` flags and what the session manager sets, and the difference between what you submitted
 * and what the driver got is invisible without printing it. The keys in [[Highlighted]] are the ones
 * that have bitten us: `partitionOverwriteMode` / `commitProtocolClass` decide whether an overwrite
 * really replaces a partition (run_history duplicates), and `fs.defaultFS` decides what a
 * scheme-less path in the conf resolves against.
 *
 * Everything is logged at INFO under `com.bnp.str`, so it follows the module's normal logging.
 */
object SparkConfLogger {

  private val log = LoggerFactory.getLogger(getClass)

  /** Settings called out individually because a wrong value silently changes behaviour. */
  private val Highlighted = Seq(
    "spark.sql.sources.partitionOverwriteMode",
    "spark.sql.sources.commitProtocolClass",
    "spark.sql.catalogImplementation",
    "spark.sql.warehouse.dir",
    "spark.serializer"
  )

  /** Hadoop-side settings that decide where a scheme-less path lands and how a job commits. */
  private val HighlightedHadoop = Seq(
    "fs.defaultFS",
    "mapreduce.fileoutputcommitter.algorithm.version"
  )

  private val SecretKey = "(?i).*(password|secret|token|credential|access[._-]?key).*".r

  /** Never log a credential that happens to sit in the Spark conf. */
  private def redact(key: String, value: String): String =
    key match {
      case SecretKey(_) => "*** redacted ***"
      case _            => value
    }

  /**
   * Log the runtime identity, the settings worth calling out, then the full SparkConf.
   * Guarded: a diagnostic must never be the reason a job fails.
   */
  def logEffectiveConf(spark: SparkSession): Unit =
    try {
      val sc = spark.sparkContext

      log.info(s"[conf] Spark ${sc.version} | master=${sc.master} | deployMode=${sc.deployMode} | " +
        s"applicationId=${sc.applicationId} | user=${sc.sparkUser}")

      Highlighted.foreach(key => log.info(s"[conf] $key = ${spark.conf.get(key, "<unset>")}"))

      val hadoopConf = sc.hadoopConfiguration
      HighlightedHadoop.foreach(key =>
        log.info(s"[conf] (hadoop) $key = ${Option(hadoopConf.get(key)).getOrElse("<unset>")}"))

      val all = sc.getConf.getAll.sortBy(_._1)
      log.info(s"[conf] full SparkConf — ${all.length} entries:")
      all.foreach { case (key, value) => log.info(s"[conf]   $key = ${redact(key, value)}") }
    } catch {
      case e: Throwable =>
        log.warn(s"[conf] could not log the Spark configuration: ${e.getClass.getSimpleName}: ${e.getMessage}")
    }
}
