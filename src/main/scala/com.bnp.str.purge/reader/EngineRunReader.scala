package com.bnp.str.purge.reader

import com.bnp.str.purge.engine.{EngineDescriptor, EngineRun}
import com.bnp.str.purge.utility.{DateUtils, PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.util.Properties

/**
 * Turns `purge_app.engine` — an engine name and the run configurations to purge — into the concrete
 * objects a purge acts on.
 *
 * This is the engine-aware entry into the existing machinery: what comes out is ordinary
 * [[PrimaryReader.ScopeEntry]] rows, so the selection SQL, the controls and the manifest need no
 * new concepts. A run is expanded into, for each output table the run declares:
 *
 *  - a metastore identity `(database, table, runid=<uuid>)`, which matches when Hive knows the
 *    partition, and
 *  - a path `<database location>/<table>/runId=<uuid>`, which matches when it does not.
 *
 * Both shapes above are the PARTITION-granular ones. An engine whose run IS the table (the
 * simulator, per Q1) expands instead to `(database, table, '')` and `<database location>/<table>`.
 * The difference lives in [[EngineRun.relativePathOf]] and [[EngineRun.partitionSpecOf]] so that
 * this reader never has to assume one shape — assuming the partition shape for a table-granular
 * engine would build paths that were never written, and a scope matching nothing is indistinguishable
 * from a run with nothing left to purge.
 *
 * BOTH are emitted, on purpose. The metastore and the filesystem disagree more often than anyone
 * would like — a partition dropped from the catalogue but still on disk is precisely the kind of
 * leftover a purge exists to remove, and one registered but already gone must still be reported
 * rather than silently missed. Matching on either side means neither disagreement hides an object.
 */
class EngineRunReader()(implicit sparkSession: SparkSession, conf: Config) {

  import EngineRunReader._

  private val log = LoggerFactory.getLogger(this.getClass)

  private val engineConfigPath = s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.ENGINE}"

  private val engineConfig: Config =
    if (conf.hasPath(engineConfigPath)) conf.getConfig(engineConfigPath) else ConfigFactory.empty()

  /** True when this run is engine-driven; false for a purely policy- or path-driven purge. */
  val isConfigured: Boolean =
    conf.hasPath(engineConfigPath) &&
      PrimaryUtilities.getStringOr(conf.getConfig(engineConfigPath), "name", "").trim.nonEmpty

  /**
   * The descriptor of the engine this run is about, when one is configured.
   *
   * Resolved from `engine.name` rather than assumed, because everything that depends on it —
   * how a run expands into a scope, where its identity sits in a path — differs per engine, and
   * an engine the registry does not know must fail loudly rather than be read as projection.
   */
  private[reader] lazy val descriptor: Option[EngineDescriptor] =
    Some(PrimaryUtilities.getStringOr(engineConfig, "name", "").trim)
      .filter(_.nonEmpty)
      .map(EngineDescriptor.of)

  /** The runs named by `purge_app.engine.runConfPaths`, read through their own configuration files. */
  lazy val runs: Seq[EngineRun] = if (!isConfigured) Seq.empty else readRuns()

  /**
   * The scope entries the runs expand to — what the selection SQL matches candidates against.
   */
  lazy val scopeEntries: Seq[PrimaryReader.ScopeEntry] = {
    val entries = runs.flatMap { run =>
      val location = databaseLocation(run.database)
      val businessDate = businessDateOf(run)

      // WHAT a run occupies in each of its tables is the descriptor's answer, not this reader's:
      // one partition of the table, or the table itself. See EngineRun.relativePathOf.
      val byIdentity = run.tables.map(table =>
        PrimaryReader.ScopeEntry("", run.database, table, run.partitionSpecOf, businessDate))

      val byPath = run.tables.flatMap { table =>
        pathOf(run, table, location).map(path =>
          PrimaryReader.ScopeEntry(path, "", "", "", businessDate))
      }

      // The run's non-Hive output folder: written by this run, named by this run's configuration.
      val outputs = run.outputDirectories.map(directory =>
        PrimaryReader.ScopeEntry(PrimaryUtilities.qualifyPath(directory), "", "", "", businessDate))

      if (location.isEmpty && !run.isTableGranular)
        log.warn(s"Could not resolve the location of database '${run.database}'; run ${run.runId} " +
          "is scoped by metastore identity only. A partition that exists on disk but not in the " +
          "catalogue will not be found.")

      byIdentity ++ byPath ++ outputs
    }.distinct

    if (entries.nonEmpty)
      log.info(s"Engine scope: ${runs.size} run(s) expanded to ${entries.size} scope entr(y|ies)")
    entries
  }

  /**
   * Everything the runs declare as INPUT, plus their history tables — objects that are inside the
   * blast radius of a badly drawn scope and are never a run's own data to delete. Control PC14
   * refuses any candidate that is one of these, or that contains one.
   */
  lazy val protectedPaths: Seq[String] = {
    val inputs = runs.flatMap(_.inputPaths).map(PrimaryUtilities.qualifyPath)
    val histories = runs.flatMap(run => databaseLocation(run.database).map { root =>
      PrimaryUtilities.normalizePath(s"$root/${tableNameOf(run.historyTable)}")
    })
    val all = (inputs ++ histories).filter(_.nonEmpty).distinct
    if (all.nonEmpty) log.info(s"${all.size} path(s) protected as shared inputs or run history")
    all
  }

  /**
   * The engine's OWN run history, normalised to the columns the controls read.
   *
   * The shared `run_history` of the STR jar knows nothing about a projection run; the engine keeps
   * its own, and that is where "is this run still going?" is actually answered. `base_folder_name`
   * carries the run's partition directory (`runId=<uuid>`) because that is the token PC04 looks for
   * in a candidate's path.
   */
  def historyEntries(): Seq[PrimaryReader.RunHistoryEntry] =
    historyTables.flatMap { table =>
      try readHistory(table)
      catch {
        case e: Throwable =>
          log.warn(s"Could not read the engine run history '$table' " +
            s"(${e.getClass.getSimpleName}: ${e.getMessage}); PC04 will not see this engine's runs")
          Seq.empty[PrimaryReader.RunHistoryEntry]
      }
    }

  /**
   * The engine's history as the run CATALOGUE wants it: one row per run id, carrying what the IHM
   * needs to describe a run to a person.
   *
   * `used_conf` is the reason this exists. `dbprojection.run_history` records the path of the
   * `conf.properties` each run was launched with — which is exactly what a purge of that run needs,
   * and what the IHM would otherwise have to track separately and keep in step. Reading it from the
   * engine's own history means the scope screen can hand the analysis its configuration without
   * anybody bookkeeping.
   *
   * Columns are resolved defensively: this table is written by another team, and a purge must not
   * break because a column was added or renamed.
   */
  def historyForCatalog(): Option[DataFrame] =
    historyTables.headOption.flatMap { table =>
      try {
        val history = sparkSession.table(table)
        val present = history.columns.map(c => c.toLowerCase -> c).toMap

        def optional(name: String): Column =
          present.get(name).map(col).getOrElse(lit(null).cast("string")).cast("string").as(name)

        val runIdColumn = Seq("run_id", "runid", "id").flatMap(present.get).headOption
        if (runIdColumn.isEmpty) return None

        val started = present.get("creation_date").map(col).getOrElse(lit(null).cast("timestamp"))
        val ended = present.get("end_date").map(col).getOrElse(lit(null).cast("timestamp"))

        // One row per run id. `run_history` has no uniqueness constraint and the same run id has
        // been seen twice with two application ids, so the newest row wins for the descriptive
        // fields — and `unfinished` is true if ANY row for that run has no end, which is the
        // reading that errs towards not deleting.
        Some(history
          .select(
            col(runIdColumn.get).cast("string").as("run_id"),
            started.as("started_at"),
            ended.as("ended_at"),
            optional("status"), optional("used_conf"), optional("run_type"),
            optional("real_user_id"), optional("user_launcher"), optional("motor"))
          .groupBy(col("run_id"))
          .agg(
            max(col("started_at")).as("started_at"),
            max(col("ended_at")).as("ended_at"),
            (sum(when(col("ended_at").isNull, 1).otherwise(0)) > 0).as("unfinished"),
            count(lit(1)).as("history_rows"),
            max(struct(col("started_at"), col("status"))).getField("status").as("history_status"),
            max(struct(col("started_at"), col("used_conf"))).getField("used_conf").as("used_conf"),
            max(struct(col("started_at"), col("run_type"))).getField("run_type").as("run_type"),
            max(struct(col("started_at"), col("real_user_id"))).getField("real_user_id").as("real_user_id")))
      } catch {
        case e: Throwable =>
          log.warn(s"Could not read '$table' for the run catalogue " +
            s"(${e.getClass.getSimpleName}: ${e.getMessage}); the catalogue will describe the " +
            "filesystem only")
          None
      }
    }

  /**
   * The database(s) this run is about, by NAME — read from the run's own configuration
   * (`output.database.name`), never from the purge configuration.
   *
   * The run configuration already says which database the engine wrote to, so naming it again here
   * would be the same duplication as naming the database's path: two answers to one question, and
   * one of them free to be wrong.
   *
   * This is also why the run CATALOGUE takes a run configuration even though it lists runs rather
   * than acting on one. It needs to know which database to look in, and the honest way to learn that
   * is to read it from a run of that database — TWIST has every run's configuration, and
   * `run_history.used_conf` records the path of each.
   */
  def databaseNames: Seq[String] =
    if (hasRunConfPaths) runs.map(_.database).filter(_.nonEmpty).distinct else Seq.empty

  /**
   * Where those databases live, asked of the metastore.
   *
   * This is what `scan.roots` defaults to for an engine-driven run: the directories an engine's
   * output actually occupies are the databases it writes to, and the catalogue already knows where
   * those are. Writing them into the purge configuration by hand would be the same duplication as
   * `engine.databaseLocation`, one level up.
   */
  def databaseRoots: Seq[String] = databaseNames.flatMap(databaseLocation).distinct

  /**
   * Which history table(s) to read.
   *
   * `engine.historyTable` when the configuration names one, and only otherwise the tables the runs
   * themselves declare. The distinction matters for the run CATALOGUE, which lists what an engine
   * left on disk and therefore has no run configurations to derive anything from — asking it for
   * one would be asking for the answer before the question.
   */
  private[purge] def historyTables: Seq[String] = {
    val configured = PrimaryUtilities.getStringOr(engineConfig, "historyTable", "").trim
    if (configured.nonEmpty) Seq(configured)
    else if (hasRunConfPaths) runs.map(_.historyTable).filter(_.nonEmpty).distinct
    else Seq.empty
  }

  /** Whether a run configuration was named, without demanding that it be read. */
  private def hasRunConfPaths: Boolean =
    PrimaryUtilities.getStringList(engineConfig, "runConfPaths").nonEmpty ||
      PrimaryUtilities.getStringOr(engineConfig, "runConfPath", "").trim.nonEmpty

  /**
   * The date a run's output is ABOUT — its as-of quarter, resolved to the last day of that quarter.
   *
   * This is what a retention on run-partitioned data has to be measured against. The files carry the
   * date the run was EXECUTED, and a 2023Q1 projection re-run last week is still 2023Q1 data: aging
   * it from the file would keep an old vintage alive forever, which is the same failure the design
   * already avoids for `as_of_date` partitions. Empty when the run declares no as-of quarter, and
   * the age then falls back to the file date.
   */
  private[reader] def businessDateOf(run: EngineRun): String =
    DateUtils.parseBusinessDate(run.asOfDateQuarter).map(_.toString).getOrElse("")

  // ---------------------------------------------------------------------------------------------

  private def readRuns(): Seq[EngineRun] = {
    val engineDescriptor = descriptor.getOrElse(
      EngineDescriptor.of(PrimaryUtilities.getStringOr(engineConfig, "name", "")))

    val paths =
      PrimaryUtilities.getStringList(engineConfig, "runConfPaths") ++
        Seq(PrimaryUtilities.getStringOr(engineConfig, "runConfPath", "")).filter(_.nonEmpty)

    require(paths.nonEmpty,
      s"$engineConfigPath.name = '${engineDescriptor.name}' but no runConfPaths were given: the purge " +
        "needs the configuration of the run(s) it is meant to remove")

    val loaded = paths.distinct.map { path =>
      val run = engineDescriptor.read(readProperties(path), path)
      log.info(s"Engine run to purge — ${run.describe}")
      run
    }

    val duplicates = loaded.groupBy(_.runId).collect { case (id, rs) if rs.size > 1 => id }
    require(duplicates.isEmpty,
      s"the same run id is named by more than one run configuration: ${duplicates.mkString(", ")}")

    loaded
  }

  /**
   * Read a run configuration, and say so when it declares the same key twice with two values.
   *
   * `Properties.load` keeps the last of them silently. In the classic simulator's configuration
   * `input.path.projection` is declared twice with DIFFERENT files, so one of the two disappears —
   * and an input that disappears is an input PC14 will not protect, on a run whose whole safety
   * argument is that shared inputs are never deleted. The engine still reads the file the standard
   * way; it no longer does so quietly.
   */
  private def readProperties(path: String): Map[String, String] = {
    import scala.collection.JavaConverters._

    val text = {
      val reader = PrimaryUtilities.getHdfsReader(path)(sparkSession.sparkContext)
      try {
        val buffer = new StringBuilder
        val chunk = new Array[Char](8192)
        var read = reader.read(chunk)
        while (read >= 0) {
          buffer.appendAll(chunk, 0, read)
          read = reader.read(chunk)
        }
        buffer.toString
      } finally reader.close()
    }

    warnOnConflictingKeys(path, text)

    val properties = new Properties()
    properties.load(new java.io.StringReader(text))
    properties.asScala.toMap
  }

  /**
   * Say so when a configuration declares the same key twice with two different values.
   *
   * `Properties.load` keeps the last of them silently. The classic simulator's configuration
   * declares `input.path.projection` twice, naming two different files, so one of them disappears —
   * and an input that disappears is an input PC14 will not protect, on an engine whose whole safety
   * argument is that shared inputs are never deleted. The file is still read the standard way; it is
   * no longer read quietly.
   */
  private def warnOnConflictingKeys(path: String, text: String): Unit = {
    val conflicting = scala.io.Source.fromString(text).getLines()
      .map(_.trim)
      .filter(line => line.nonEmpty && !line.startsWith("#") && line.contains("="))
      .map { line =>
        val separator = line.indexOf('=')
        line.substring(0, separator).trim -> line.substring(separator + 1).trim
      }
      .toSeq
      .groupBy { case (key, _) => key }
      .collect { case (key, pairs) if pairs.map { case (_, v) => v }.distinct.size > 1 => key }
      .toSeq
      .sorted

    if (conflicting.nonEmpty)
      log.warn(s"$path declares ${conflicting.size} key(s) twice with different values; only the " +
        s"last of each is read and the other is lost: ${conflicting.mkString(", ")}")
  }

  /**
   * Where a Hive database's tables live — **asked of the metastore**, the same way
   * `com.bnp.str.utilities.audit.RunAudit` derives its own table location.
   *
   * The metastore is authoritative and there is deliberately nothing to configure on the cluster.
   * A database's path written into a purge configuration is a second copy of an answer the
   * catalogue already holds, and a copy that will drift: the day a database is moved, the purge is
   * still pointed at yesterday's directory — finding nothing, or worse, finding something else.
   *
   * `engine.databaseLocation` survives only as a fallback for a session with no metastore to ask,
   * and saying so costs a warning every time it is used.
   */
  /**
   * Where one of a run's tables actually put its data.
   *
   * For a partition-granular engine this is `<database location>/<table>/runId=<uuid>` — the table
   * sits under its database, and deriving it costs no metastore call per table.
   *
   * A TABLE-granular engine cannot be read that way. The classic simulator's outputs are EXTERNAL
   * tables whose location is a production output tree (`…/Simulateur_Production/Output_Simulateur/
   * facs_<scenario>_<t>/…`) entirely outside `dbsimulateur.db`, so `<database location>/<table>`
   * names a directory that was never written. Its registered LOCATION is asked of the metastore
   * instead — which also resolves the `%s`/`%t` placeholders the configuration carries, without this
   * engine having to know what they stand for.
   */
  private def pathOf(run: EngineRun, table: String, databaseRoot: Option[String]): Option[String] = {
    if (!run.isTableGranular)
      return databaseRoot.map(root => PrimaryUtilities.normalizePath(s"$root/${run.relativePathOf(table)}"))

    tableLocation(run.database, table).orElse {
      // No fallback to `<database location>/<table>` on purpose. For this shape of engine that path
      // is very probably wrong — the tables are external and written elsewhere — and a wrong path in
      // a purge scope is worse than no path: it matches nothing while looking like coverage. The
      // metastore identity entry still covers the table. The commonest reason to land here is
      // benign: a `_nosecto` variant the run never wrote.
      log.info(s"The metastore holds no location for '${run.database}.$table'; it is scoped by " +
        "metastore identity only (a variant that was never written simply does not exist)")
      None
    }
  }

  /** A table's registered location, or None when the metastore does not hold the table. */
  private def tableLocation(database: String, table: String): Option[String] =
    try {
      val catalog = sparkSession.sessionState.catalog
      val identifier = org.apache.spark.sql.catalyst.TableIdentifier(table, Some(database))
      if (!catalog.tableExists(identifier)) None
      else catalog.getTableMetadata(identifier).storage.locationUri
        .map(uri => PrimaryUtilities.normalizePath(uri.toString))
    } catch {
      case e: Throwable =>
        log.warn(s"Metastore lookup of table '$database.$table' failed " +
          s"(${e.getClass.getSimpleName}: ${e.getMessage})")
        None
    }

  private def databaseLocation(database: String): Option[String] = {
    val fromMetastore =
      try Some(PrimaryUtilities.normalizePath(sparkSession.catalog.getDatabase(database).locationUri))
      catch {
        case e: Throwable =>
          log.warn(s"Metastore lookup of database '$database' failed " +
            s"(${e.getClass.getSimpleName}: ${e.getMessage})")
          None
      }

    fromMetastore.orElse {
      val configured = PrimaryUtilities.getStringOr(engineConfig, "databaseLocation", "")
      if (configured.isEmpty) None
      else {
        log.warn(s"Falling back to the configured engine.databaseLocation for '$database'. This is " +
          "a copy of what the metastore should have answered, and it will drift from it.")
        Some(PrimaryUtilities.qualifyPath(configured))
      }
    }
  }

  /** Read one engine history table, mapping whatever it calls its columns onto the shared shape. */
  private def readHistory(table: String): Seq[PrimaryReader.RunHistoryEntry] = {
    import org.apache.spark.sql.functions.{coalesce, col, lit}

    val history = sparkSession.table(table)
    val columns = history.columns.map(_.toLowerCase).toSet

    def firstOf(candidates: String*): Option[String] =
      candidates.find(candidate => columns.contains(candidate.toLowerCase))

    val runIdColumn = firstOf("run_id", "runid", "id")
    require(runIdColumn.isDefined, s"'$table' has no run id column")

    val statusColumn = firstOf("status", "run_status", "state")
    val startColumn = firstOf("creation_date", "start_date", "start_time")
    val endColumn = firstOf("end_date", "end_time")

    history
      .select(
        coalesce(col(runIdColumn.get).cast("string"), lit("")).as("run_id"),
        statusColumn.map(c => coalesce(col(c).cast("string"), lit(""))).getOrElse(lit("")).as("status"),
        startColumn.map(c => col(c).cast("timestamp")).getOrElse(lit(null).cast("timestamp")).as("creation_date"),
        endColumn.map(c => col(c).cast("timestamp")).getOrElse(lit(null).cast("timestamp")).as("end_date"))
      .collect()
      .map { row =>
        val runId = row.getString(0)
        PrimaryReader.RunHistoryEntry(
          run_id = runId,
          module_name = descriptor.map(_.name).getOrElse(""),
          status = row.getString(1),
          creation_date = row.getAs[java.sql.Timestamp]("creation_date"),
          end_date = Option(row.getAs[java.sql.Timestamp]("end_date")),
          // The token PC04 looks for inside a candidate's path. Where a run's identity sits in a
          // path is a per-engine, per-granularity fact, so the descriptor answers it: a
          // partition-granular engine puts it in `runId=<uuid>`, a table-granular one in the table
          // name. Hardcoding the partition spelling here would make PC04 — the control that stops a
          // purge deleting under a live writer — silently unable to match anything.
          base_folder_name = descriptor.map(_.runToken(runId)))
      }
      .toList
  }
}

object EngineRunReader {

  /** `dbprojection.run_history` -> `run_history`. */
  private[reader] def tableNameOf(qualified: String): String =
    Option(qualified).getOrElse("").split('.').lastOption.map(_.trim).getOrElse("")
}
