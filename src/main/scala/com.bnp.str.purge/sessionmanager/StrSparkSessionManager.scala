// Package declaration which organizes the code modules, similar to a folder structure.
package com.bnp.str.purge.sessionmanager

import com.bnp.str.utilities.SparkConfLogger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Singleton object to manage Spark Session creation for the purge engine.
 *
 * One factory for BOTH local and cluster — no `isLocalhost` flag. The runtime environment
 * decides: when the job is launched via `spark-submit` the cluster sets `spark.master` (yarn,
 * k8s, …) and provides the Hive metastore / warehouse, so those are kept untouched. When no
 * master is supplied (IDE run, unit test, laptop) or it is an explicit `local[…]`, a
 * self-contained local session is built (in-memory Derby metastore, local warehouse). The same
 * `enableHiveSupport()` works in both cases — the purge engine needs it because
 * [[com.bnp.str.purge.reader.CatalogReader]] walks the metastore.
 *
 * Identical to the session manager of the other modules, with one purge-specific addition: the
 * HDFS Trash interval is NOT set here. It is a cluster-wide setting (`fs.trash.interval` in
 * core-site.xml) and the engine deliberately reads it rather than imposing one, because the
 * restore window it defines is a cluster guarantee, not a job parameter.
 */
object StrSparkSessionManager {

  /**
   * Fetches or creates a SparkSession suited to wherever it runs (local or cluster), decided from
   * the environment rather than a caller-supplied flag.
   *
   * @param appName The name of the application. This name will be displayed in the Spark UI.
   * @return An instance of SparkSession tailored for the detected environment.
   */
  def fetchSparkSession(appName: String): SparkSession = {

    // `new SparkConf()` loads the `spark.*` system properties / SPARK_ env that spark-submit sets.
    // No master, or an explicit `local[…]` master, means a local run; anything else is a cluster.
    val isLocal = new SparkConf().getOption("spark.master").forall(_.startsWith("local"))

    val builder = SparkSession
      .builder()
      .appName(appName)
      // ---- common to both environments ----
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")

    val tuned =
      if (isLocal) {
        // ---- self-contained local run: embedded Derby metastore + local warehouse, no cluster deps ----
        val warehouse = s"${System.getProperty("user.dir")}/out/warehouse"
        builder
          .master("local[*]")
          .config("spark.driver.bindAddress", "0.0.0.0")
          .config("spark.driver.host", "127.0.0.1")
          .config("spark.broadcast.compress", "false")
          .config("spark.sql.codegen.wholeStage", "false")
          .config("spark.debug.maxToStringFields", 1000)
          .config("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:db;create=true")
          .config("spark.sql.warehouse.dir", warehouse)
          .config("hive.metastore.warehouse.dir", warehouse)
      } else {
        // ---- cluster: spark-submit provides master / queue / metastore; only set engine tuning ----
        builder
          .config("hive.execution.engine", "spark")
          .config("spark.sql.autoBroadcastJoinThreshold", 1073741824L)
      }

    // getOrCreate manages resource efficiency and ensures a single session per JVM.
    val session = tuned
      .enableHiveSupport()
      .getOrCreate()

    SparkConfLogger.logEffectiveConf(session)
    session
  }

}
