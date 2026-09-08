package com.bnp.str.purge.engine

import org.slf4j.LoggerFactory

/**
 * The classic simulator, and the first TABLE-granular engine.
 *
 * A projection run writes `runId=<uuid>` into thirty-odd standing tables. A simulator run does not:
 * it CREATES tables of its own, named after the simulation, and purging the run means removing those
 * tables outright.
 *
 * {{{
 *   runId=5bbad7c8-8e7d-4e88-8106-f00ff4345e0b
 *   parameter.asOfDateQuarter=2026Q2
 *   global.output.database.name=dbsimulateur
 *   output.externalTable.nameFacilityOutput=dbsimulateur.STAPO_sp_starting_point_…_20260904_fac
 *   output.externalTable.nameFacilityMeasurementByTermOutput=dbsimulateur.STAPO_…_fac_measure_ByTermOutput
 *   run.history.tablename=RUN_HISTORY
 * }}}
 *
 * The table name carries the whole identity of the simulation — category, starting-point portfolio,
 * projection id, parameter id and launch date — and carries neither the run id nor TWIST's
 * simulation id. That is why nothing here is derived from the shape of a name: the tables are read
 * from the four keys that declare them and from nowhere else.
 *
 * Four readings of this file are not obvious, and each is a way to get a purge wrong. See
 * `docs/purge/simulator_classic_analysis.md` for the full account.
 *
 *  1. **Only four keys name a purgeable table**, and they are enumerated in [[OUTPUT_TABLE_KEYS]]
 *     rather than found by prefix. `output.externalTable.name` and the three
 *     `output.monte.carlo.externalTable.name*` keys sit in the same namespace and are TEMPLATES
 *     (`output_%s_%t`); collecting them by prefix would put an unresolvable pattern in a purge scope.
 *
 *  2. **No output DIRECTORY is ever collected.** `output.path.run.directory` is the root of every
 *     simulator run ever written — not this run's output — and the per-run `output.path.exportdir*`
 *     values carry `%s`/`%t` placeholders that only the metastore can resolve. The data is reached
 *     through the tables' registered locations instead, which is the one answer that is already
 *     correct. A purge of this engine therefore removes TABLES, and nothing that is only a path.
 *
 *  3. **Table names are database-qualified and mixed-case.** The conf spells
 *     `dbsimulateur.STAPO_sp_…`; Hive holds the database and the table separately, and lower-cases
 *     the name. Both are split and lower-cased here, for the same reason projection lower-cases
 *     `projected_CR_detailed`: a name Hive does not recognise finds nothing, and a purge that
 *     silently finds nothing looks exactly like a purge that had nothing to do.
 *
 *  4. **`run.history.tablename` is NOT qualified.** It reads `RUN_HISTORY`, with no database, where
 *     projection declares `dbprojection.run_history` in full. It is qualified here with the run's own
 *     output database, because that is the only database this configuration names. PC04 reads that
 *     table to find out whether a job is still writing, so an unqualified name would leave the
 *     control unable to resolve anything and reporting UNKNOWN for every simulator purge.
 */
object SimulatorClassicEngine extends EngineDescriptor {

  private val log = LoggerFactory.getLogger(this.getClass)

  override val name: String = "simulator_classic"

  /**
   * Empty on purpose: a run of this engine is not a partition of anything, so there is no partition
   * key for it to be written under. [[EngineRun.partitionSpecOf]] and [[EngineRun.relativePathOf]]
   * never consult it for a table-granular run.
   */
  override val partitionKey: String = ""

  override val granularity: String = EngineDescriptor.GRANULARITY_TABLE

  /**
   * The only keys whose value is a table a purge of this engine may remove — confirmed by the
   * business on 2026-09-07.
   *
   * Enumerated rather than matched by prefix, and that is the whole point: the neighbouring
   * `output.externalTable.name` is the template `output_%s_%t`, and `global.output.table.name` is
   * the SHARED results table that every run writes into and no run owns. A prefix scan would collect
   * both.
   */
  private[engine] val OUTPUT_TABLE_KEYS = Seq(
    "output.externalTable.nameFacilityOutput",
    "output.externalTable.nameFacilityWeightedOutput",
    "output.externalTable.nameFacilityMeasurementOutput",
    "output.externalTable.nameFacilityMeasurementByTermOutput")

  /**
   * Tables the engine writes that belong to no single run, and that a purge must never remove.
   *
   * `global.output.table.name` is the results table EVERY run writes into — one run's purge must not
   * take another's rows with it — and `global.output.table.metadata` is the run metadata. Alongside
   * `run.history.tablename` they are the record that the runs happened at all, which is also the
   * evidence that a purge of one of them was legitimate. Confirmed by the business on 2026-09-08:
   * the history is never purged.
   */
  private[engine] val SHARED_TABLE_KEYS = Seq(
    "global.output.table.name",     // simulation_results_fac_partitioned_full
    "global.output.table.metadata") // run_metadata

  override def read(properties: Map[String, String], confPath: String): EngineRun = {
    val database = value(properties, "global.output.database.name").toLowerCase

    val runId = value(properties, "runId")
    require(runId.nonEmpty, s"$confPath declares no 'runId': there is no run to purge")
    require(database.nonEmpty,
      s"$confPath declares no 'global.output.database.name': the tables cannot be resolved")

    val tables = withNonSectoVariants(outputTables(properties, database, confPath), properties)
    require(tables.nonEmpty,
      s"$confPath names no output table in [${OUTPUT_TABLE_KEYS.mkString(", ")}]; a purge of this " +
        "run would have nothing to remove, which is more likely a misread configuration than a run " +
        "that produced nothing")

    EngineRun(
      engine = name,
      runId = runId,
      // The simulation's NAME lives in TWIST, not in the file it generates. Left empty rather than
      // reconstructed from the table name: what looks like a naming convention here is data.
      runName = "",
      runType = firstOf(properties, "run.simulation.category", "run.type"),
      launchedBy = value(properties, "run.realUserId"),
      asOfDateQuarter = value(properties, "parameter.asOfDateQuarter"),
      database = database,
      tables = tables,
      partitionKey = partitionKey,
      granularity = granularity,
      historyTable = historyTable(properties, database),
      // Deliberately empty — see reading 2 in the class comment. This engine purges tables.
      outputDirectories = Seq.empty,
      inputPaths = inputPaths(properties),
      confPath = confPath,
      protectedTables = protectedTables(properties))
  }

  /**
   * The run's own tables, unqualified and lower-cased, with the traps of the four keys handled:
   * an empty value (`nameFacilityMeasurementOutput=` when that output is switched off), the same
   * table named twice (`nameFacilityOutput` and `nameFacilityWeightedOutput` hold one value), and a
   * value still carrying a `%s`/`%t` template.
   */
  private def outputTables(properties: Map[String, String],
                           database: String,
                           confPath: String): Seq[String] =
    OUTPUT_TABLE_KEYS
      .map(key => key -> value(properties, key))
      .filter { case (_, declared) => declared.nonEmpty }
      .flatMap { case (key, declared) =>
        if (declared.contains("%")) {
          // A template is not a table. Refused rather than expanded: the engine does not know what
          // %t stands for, and a guess would name a table that either does not exist or is not this
          // run's.
          log.warn(s"$confPath: $key = '$declared' still carries a template placeholder; " +
            "it is not a resolved table name and is left out of the scope")
          None
        } else Some(split(declared, database, key, confPath))
      }
      .distinct

  /**
   * The engine's own tables that no run owns — never purged, and named so PC14 can refuse a scope
   * that reached one however it was drawn.
   */
  private def protectedTables(properties: Map[String, String]): Seq[String] =
    SHARED_TABLE_KEYS
      .map(key => value(properties, key))
      .filter(_.nonEmpty)
      .map(table => table.substring(table.indexOf('.') + 1).toLowerCase)
      .distinct

  /**
   * Every output table may exist twice: once as declared, and once for the non-sectoral perimeter
   * under the suffix `output.suffix.for_non_secto_table` names.
   *
   * The four output keys name only the first of each pair, so a purge built from them alone removes
   * half of what the run wrote and leaves the `_nosecto` twin behind — a run half-removed, which
   * §3bis of the specification calls a thing no consumer can interpret and no report can explain.
   * The suffix is read from the configuration rather than assumed, and a table already carrying it
   * is not suffixed twice.
   *
   * A variant that was never written costs nothing: it is not in the metastore, so it resolves to no
   * location, matches no candidate, and simply does not appear in the manifest. Scoping one that
   * might exist is therefore the safe direction of the two.
   */
  private def withNonSectoVariants(tables: Seq[String],
                                   properties: Map[String, String]): Seq[String] = {
    val suffix = value(properties, "output.suffix.for_non_secto_table").stripPrefix("_")
    if (suffix.isEmpty) return tables

    val marker = s"_${suffix.toLowerCase}"
    val variants = tables.filterNot(_.endsWith(marker)).map(table => s"$table$marker")
    if (variants.nonEmpty)
      log.info(s"${variants.size} non-sectoral variant(s) added to the scope under '$marker'; " +
        "each is purged only if it exists")
    (tables ++ variants).distinct
  }

  /** `dbsimulateur.STAPO_…_fac` -> `stapo_…_fac`, checking the database it claims to be in. */
  private def split(qualified: String, database: String, key: String, confPath: String): String = {
    val separator = qualified.indexOf('.')
    if (separator < 0) return qualified.toLowerCase

    val declaredDatabase = qualified.substring(0, separator).toLowerCase
    val table = qualified.substring(separator + 1).toLowerCase
    require(declaredDatabase == database,
      s"$confPath: $key names a table in '$declaredDatabase' but the run's database is " +
        s"'$database'. A purge will not cross databases on the strength of a key it was not told " +
        "to expect.")
    table
  }

  /**
   * `RUN_HISTORY` -> `dbsimulateur.run_history`.
   *
   * Qualified with the run's own output database, which is the only database this configuration
   * names. An already-qualified value is left alone.
   */
  private def historyTable(properties: Map[String, String], database: String): String = {
    val declared = value(properties, "run.history.tablename")
    if (declared.isEmpty) ""
    else if (declared.contains(".")) declared.toLowerCase
    else s"$database.${declared.toLowerCase}"
  }

  /**
   * Every path this run READ — protected by PC14, never this run's to delete.
   *
   * Collected from any `input.*` key holding an absolute path, not from `input.path.*` alone: this
   * engine also declares inputs under `input.lgd_forward_looking.*.path`, and a prefix rule copied
   * from projection would have missed five of them — five shared referentials left unprotected.
   */
  private def inputPaths(properties: Map[String, String]): Seq[String] =
    properties
      .filterKeys(_.startsWith("input."))
      .values
      .map(_.trim)
      .filter(_.startsWith("/"))
      .toSeq
      .distinct
      .sorted

  private def value(properties: Map[String, String], key: String): String =
    properties.getOrElse(key, "").trim

  private def firstOf(properties: Map[String, String], keys: String*): String =
    keys.map(value(properties, _)).find(_.nonEmpty).getOrElse("")
}
