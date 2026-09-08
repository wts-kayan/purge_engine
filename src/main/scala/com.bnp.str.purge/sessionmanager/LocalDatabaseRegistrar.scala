package com.bnp.str.purge.sessionmanager

import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Registers the databases a LOCAL run needs, so that a laptop resolves a database's location the
 * same way the cluster does — by asking the metastore.
 *
 * The alternative was to let the configuration name a database's HDFS path directly. That is the
 * thing worth avoiding: the metastore already knows where a database lives, a second copy of that
 * answer in a purge configuration is a copy that will drift, and the drift shows up as a purge
 * pointed at a directory that is no longer the database's — silently finding nothing, or finding
 * something else.
 *
 * So the engine always asks the catalogue. On the cluster there is nothing to configure at all. On a
 * laptop, where the session runs against an empty in-memory metastore, this fills that metastore in
 * from `purge_app.technical.localDatabases` — one `CREATE DATABASE … LOCATION` per entry — and from
 * `purge_app.technical.localTables`, one `CREATE TABLE … LOCATION` per entry, for the engines whose
 * tables do not live under their database at all.
 *
 * It refuses to do anything on a cluster. A job that could create databases on Promethee is not what
 * anyone wants from a purge engine, and the guard is the environment rather than a flag someone
 * could set by accident.
 */
object LocalDatabaseRegistrar {

  private val log = LoggerFactory.getLogger(this.getClass)

  val TECHNICAL = "technical"
  val LOCAL_DATABASES = "localDatabases"
  val LOCAL_TABLES = "localTables"

  def register(conf: Config)(implicit spark: SparkSession): Unit = {
    registerDatabases(conf)
    registerTables(conf)
  }

  private def registerDatabases(conf: Config)(implicit spark: SparkSession): Unit = {
    import scala.collection.JavaConverters._

    val path = s"${PrimaryConstants.APP_CONF}.$TECHNICAL.$LOCAL_DATABASES"
    if (!conf.hasPath(path)) return
    if (!isLocal(path)) return

    conf.getConfigList(path).asScala.foreach { entry =>
      val name = PrimaryUtilities.getStringOr(entry, "name", "").trim
      val location = PrimaryUtilities.qualifyPath(PrimaryUtilities.getStringOr(entry, "location", ""))

      if (name.isEmpty || location.isEmpty)
        log.warn(s"$path entry ignored: both 'name' and 'location' are required")
      else
        try {
          spark.sql(s"CREATE DATABASE IF NOT EXISTS `$name` LOCATION '$location'")
          log.info(s"[local] database '$name' registered at $location")
        } catch {
          case e: Throwable =>
            log.warn(s"[local] could not register database '$name' at $location " +
              s"(${e.getClass.getSimpleName}: ${e.getMessage})")
        }
    }
  }

  /**
   * Registers local EXTERNAL tables, each at a location of its own.
   *
   * A table-granular engine — the classic simulator — writes tables whose location is nowhere near
   * their database: `dbsimulateur` lives in one place and its tables in a production output tree.
   * The engine resolves those by asking the metastore for the TABLE, so a local rehearsal is only
   * faithful if the local metastore holds the same shape. Registering the database alone would
   * leave the engine guessing `<database>/<table>`, which is exactly the wrong answer this design
   * exists to avoid — and the rehearsal would pass while the cluster failed.
   *
   * The schema is a single nominal column: nothing reads these tables, and declaring one keeps
   * Spark from inferring a schema off the files.
   */
  private def registerTables(conf: Config)(implicit spark: SparkSession): Unit = {
    import scala.collection.JavaConverters._

    val path = s"${PrimaryConstants.APP_CONF}.$TECHNICAL.$LOCAL_TABLES"
    if (!conf.hasPath(path)) return
    if (!isLocal(path)) return

    conf.getConfigList(path).asScala.foreach { entry =>
      val database = PrimaryUtilities.getStringOr(entry, "database", "").trim
      val name = PrimaryUtilities.getStringOr(entry, "name", "").trim
      val location = PrimaryUtilities.qualifyPath(PrimaryUtilities.getStringOr(entry, "location", ""))
      val format = PrimaryUtilities.getStringOr(entry, "format", "orc").trim

      if (database.isEmpty || name.isEmpty || location.isEmpty)
        log.warn(s"$path entry ignored: 'database', 'name' and 'location' are all required")
      else
        try {
          spark.sql(s"CREATE TABLE IF NOT EXISTS `$database`.`$name` (value STRING) " +
            s"USING $format LOCATION '$location'")
          log.info(s"[local] table '$database.$name' registered at $location")
        } catch {
          case e: Throwable =>
            log.warn(s"[local] could not register table '$database.$name' at $location " +
              s"(${e.getClass.getSimpleName}: ${e.getMessage})")
        }
    }
  }

  /** Refuses to touch a metastore that is not a laptop's. */
  private def isLocal(path: String)(implicit spark: SparkSession): Boolean =
    if (spark.sparkContext.isLocal) true
    else {
      log.warn(s"$path is set but this is not a local run; ignoring it. Locations come from the " +
        "metastore on the cluster, and this engine creates nothing there.")
      false
    }
}
