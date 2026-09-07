package com.bnp.str.purge.reader

import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * The engine's five inputs, resolved by name — the same switch-by-name shape as
 * `com.bnp.str.addons.reader.PrimaryReader`.
 *
 * Each one is a `lazy val`, so an input a given run does not use is never materialised: a
 * path-only scope never touches the metastore, and a run whose policies are inline never reads the
 * referential table. On a scan of a real datalake that is not a micro-optimisation — `hdfs_inventory`
 * is minutes of NameNode traffic.
 */
class PrimaryReader()(implicit sparkSession: SparkSession, conf: Config) {

  private val log = LoggerFactory.getLogger(this.getClass)

  private lazy val hdfs_inventory: DataFrame = new InventoryReader().read()

  private lazy val hive_catalog: DataFrame = new CatalogReader().read()

  private lazy val purge_policy: DataFrame = new PolicyReader().read()

  private lazy val purge_scope: DataFrame = readPurgeScope()

  private lazy val run_history: DataFrame = readRunHistory()

  /** The engine runs this purge is about, when it is engine-driven. */
  lazy val engineRuns: EngineRunReader = new EngineRunReader()

  /**
   * Paths no candidate may touch whatever the scope says: the shared inputs the runs declare, and
   * the engine's own run history. Consumed by control PC14.
   */
  def protectedPaths(): Seq[String] = engineRuns.protectedPaths

  def getMappingReader(input: String): DataFrame = input match {
    case PrimaryConstants.HDFS_INVENTORY => hdfs_inventory
    case PrimaryConstants.HIVE_CATALOG   => hive_catalog
    case PrimaryConstants.PURGE_POLICY   => purge_policy
    case PrimaryConstants.PURGE_SCOPE    => purge_scope
    case PrimaryConstants.RUN_HISTORY    => run_history
    case _ => throw new IllegalArgumentException(
      s"Invalid input '$input'. Expected one of: ${PrimaryConstants.HDFS_INVENTORY}, " +
        s"${PrimaryConstants.HIVE_CATALOG}, ${PrimaryConstants.PURGE_POLICY}, " +
        s"${PrimaryConstants.PURGE_SCOPE}, ${PrimaryConstants.RUN_HISTORY}")
  }

  /**
   * The explicit selection a person made in TWIST, as `(path)` or `(database, table, spec)` rows.
   *
   * An entry names EITHER a path OR a partition, never both, so the selection SQL can tell which
   * comparison to make. An absent or disabled block yields an empty frame, which is the normal
   * shape of a policy-driven run.
   */
  private def readPurgeScope(): DataFrame = {
    import sparkSession.implicits._
    import scala.collection.JavaConverters._

    val path = s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.PURGE_SCOPE}"
    val scopeConfig = if (conf.hasPath(path)) conf.getConfig(path) else ConfigFactory.empty()
    val handPicked =
      PrimaryUtilities.getBooleanOr(scopeConfig, PrimaryConstants.PURGE_SCOPE_ENABLE, default = false)

    // An engine-driven purge HAS an explicit scope — the runs it names — whether or not anyone also
    // hand-picked paths, so `purge_scope.enable` does not gate it.
    if (!handPicked && !engineRuns.isConfigured) {
      log.info(s"$path.enable = false and no engine configured; policy-driven run")
      return sparkSession.emptyDataset[PrimaryReader.ScopeEntry].toDF()
    }

    val paths =
      if (!handPicked) Seq.empty[PrimaryReader.ScopeEntry]
      else PrimaryUtilities.getStringList(scopeConfig, "paths")
      .map(p => PrimaryReader.ScopeEntry(PrimaryUtilities.qualifyPath(p), "", "", ""))

    val partitions =
      if (!handPicked || !scopeConfig.hasPath("partitions")) Seq.empty[PrimaryReader.ScopeEntry]
      else scopeConfig.getConfigList("partitions").asScala.toList.map { entry =>
        PrimaryReader.ScopeEntry(
          scope_path = "",
          scope_database = PrimaryUtilities.getStringOr(entry, "database", "").trim,
          scope_table = PrimaryUtilities.getStringOr(entry, "table", "").trim,
          scope_partition_spec =
            PrimaryUtilities.normalizePartitionSpec(PrimaryUtilities.getStringOr(entry, "spec", "").trim))
      }

    val entries = paths ++ partitions ++ engineRuns.scopeEntries
    log.info(s"Explicit scope: ${paths.size} path(s), ${partitions.size} partition(s), " +
      s"${engineRuns.scopeEntries.size} from the engine run(s)")
    sparkSession.createDataset(entries).toDF()
  }

  /**
   * The shared `run_history`, used from P3 by PC04 to refuse a deletion under a live writer.
   *
   * Read from the ORC files rather than through the catalog, so a missing metastore entry is not a
   * reason to fail. When the location is not configured or does not exist the frame is EMPTY — and
   * PC04 will then have nothing to check, which the control reports as unknown rather than as
   * "no run is writing here". An empty run_history must never read as a clearance.
   */
  private def readRunHistory(): DataFrame = {
    import sparkSession.implicits._

    val path = s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.RUN_HISTORY}"
    val location =
      if (conf.hasPath(path)) PrimaryUtilities.getStringOr(conf.getConfig(path), "root", "") else ""

    // The engine's OWN history first: for an engine-driven purge it is the authoritative answer to
    // "is this run still going?", and the shared STR run_history knows nothing about it.
    val fromEngine = sparkSession.createDataset(engineRuns.historyEntries()).toDF()

    val fromShared =
      if (location.isEmpty) {
        log.info(s"$path.root is not set; the shared run_history contributes nothing to this run")
        sparkSession.emptyDataset[PrimaryReader.RunHistoryEntry].toDF()
      } else
        try sparkSession.read.orc(location)
        catch {
          case e: Throwable =>
            log.warn(s"Could not read the shared run_history at $location " +
              s"(${e.getClass.getSimpleName}: ${e.getMessage}); continuing without it")
            sparkSession.emptyDataset[PrimaryReader.RunHistoryEntry].toDF()
        }

    // Only the columns the controls read, so two tables with different schemas can be unioned.
    val columns = PrimaryReader.HISTORY_COLUMNS.map(col)
    if (fromEngine.head(1).isEmpty) fromShared
    else if (fromShared.head(1).isEmpty) fromEngine.select(columns: _*)
    else fromEngine.select(columns: _*).unionByName(alignHistory(fromShared), allowMissingColumns = true)
  }

  /** Project a run_history-shaped frame onto the columns the controls read, filling absent ones. */
  private def alignHistory(history: DataFrame): DataFrame = {
    val present = history.columns.map(_.toLowerCase).toSet
    history.select(PrimaryReader.HISTORY_COLUMNS.map { name =>
      if (present.contains(name)) col(name) else lit(null).cast("string").as(name)
    }: _*)
  }
}

object PrimaryReader {

  /** The run_history columns the controls read; everything else a source carries is ignored. */
  val HISTORY_COLUMNS = Seq("run_id", "module_name", "status", "creation_date", "end_date", "base_folder_name")

  /**
   * One line of the explicit selection: a path, or a partition identified in the metastore.
   *
   * @param scope_business_date the ISO date this object's data is ABOUT, when the selection knows it
   *                            — an engine run carries its as-of quarter, and that is a truer age for
   *                            run-partitioned output than any date on the files. Empty when the
   *                            selection is just a path someone picked.
   */
  final case class ScopeEntry(scope_path: String,
                              scope_database: String,
                              scope_table: String,
                              scope_partition_spec: String,
                              scope_business_date: String = "")

  /** The columns of `run_history` this engine reads; the shared table has more. */
  final case class RunHistoryEntry(run_id: String,
                                   module_name: String,
                                   status: String,
                                   creation_date: java.sql.Timestamp,
                                   end_date: Option[java.sql.Timestamp],
                                   base_folder_name: Option[String])
}
