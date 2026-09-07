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
 * from `purge_app.technical.localDatabases` — one `CREATE DATABASE … LOCATION` per entry.
 *
 * It refuses to do anything on a cluster. A job that could create databases on Promethee is not what
 * anyone wants from a purge engine, and the guard is the environment rather than a flag someone
 * could set by accident.
 */
object LocalDatabaseRegistrar {

  private val log = LoggerFactory.getLogger(this.getClass)

  val TECHNICAL = "technical"
  val LOCAL_DATABASES = "localDatabases"

  def register(conf: Config)(implicit spark: SparkSession): Unit = {
    import scala.collection.JavaConverters._

    val path = s"${PrimaryConstants.APP_CONF}.$TECHNICAL.$LOCAL_DATABASES"
    if (!conf.hasPath(path)) return

    if (!spark.sparkContext.isLocal) {
      log.warn(s"$path is set but this is not a local run; ignoring it. Database locations come " +
        "from the metastore on the cluster, and this engine does not create databases there.")
      return
    }

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
}
