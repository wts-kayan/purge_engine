package com.bnp.str.purge.engine

/**
 * The projection engine (IFRS9 / stress-test), the MVP of the engine-aware purge.
 *
 * Its run configuration is a `conf.properties` whose shape decides everything below:
 *
 * {{{
 *   projection.run.id=9df8cf3a-c2fa-4bfd-9068-aa6587ba84cf
 *   projection.name=IFRS9_scn26Q3_scenDraft
 *   projection.history.table.name=dbprojection.run_history
 *   output.database.name=dbprojection
 *   output.table.name.projectedDR=projected_DR
 *   output.table.name.termStructures.detailed=term_structure_detailed
 *   …thirty-odd of them…
 *   cluster.output.directory=/Projects/.../Output_Cluster/IHM/10463/
 *   input.path.model=/Projects/.../modelsTemplate_ifrs9_26Q2_v0.csv
 *   …
 * }}}
 *
 * Three readings in that file are not obvious and each one is a way to get a purge wrong:
 *
 *  1. **Table names are lowercased.** The conf spells `projected_CR_detailed`; Hive puts
 *     `projected_cr_detailed` on disk. Taking the conf spelling literally builds a path that does
 *     not exist, and a purge that silently finds nothing looks exactly like a purge that had
 *     nothing to do.
 *
 *  2. **`run_history` is not an output.** It is the engine's audit of every run it ever made, and
 *     the row describing the run being purged is the evidence that the purge was legitimate.
 *     It is excluded from the scope and named as protected.
 *
 *  3. **`input.path.*` are SHARED.** Models, scenarios, rating scales and idealized matrices are
 *     read by dozens of runs and belong to none of them. They are collected here so control PC14
 *     can refuse to delete them however the scope was drawn.
 */
object ProjectionEngine extends EngineDescriptor {

  override val name: String = "projection"

  /**
   * The on-disk spelling, capital I, as observed under
   * `/Projects/STCreditRisk_STE/hive/databases/dbprojection.db/term_structure`. The metastore
   * lowercases it to `runid`; [[EngineRun]] exposes both spellings so neither side has to guess.
   */
  override val partitionKey: String = "runId"

  /** A projection run is one `runId=` partition of each of its output tables. */
  override val granularity: String = EngineDescriptor.GRANULARITY_PARTITION

  private val RUN_ID = "projection.run.id"
  private val RUN_NAME = "projection.name"
  private val RUN_TYPE = "projection.run.type"
  private val TWIST_USER = "projection.twist.user"
  private val AS_OF_QUARTER = "parameter.asOfDateQuarter"
  private val HISTORY_TABLE = "projection.history.table.name"
  private val DATABASE = "output.database.name"

  private val TABLE_PREFIX = "output.table.name."
  private val INPUT_PREFIX = "input.path."
  private val OUTPUT_DIRECTORY_KEYS =
    Seq("cluster.output.directory", "output.hdfs.directory", "local.output.directory")

  override def read(properties: Map[String, String], confPath: String): EngineRun = {
    val runId = value(properties, RUN_ID)
    require(runId.nonEmpty,
      s"$confPath: '$RUN_ID' is missing — without a run id there is nothing to identify the run by")

    val database = value(properties, DATABASE)
    require(database.nonEmpty, s"$confPath: '$DATABASE' is missing — no idea which database to purge")

    val historyTable = value(properties, HISTORY_TABLE)

    EngineRun(
      engine = name,
      runId = runId,
      runName = value(properties, RUN_NAME),
      runType = value(properties, RUN_TYPE),
      launchedBy = value(properties, TWIST_USER),
      asOfDateQuarter = value(properties, AS_OF_QUARTER),
      database = database,
      tables = outputTables(properties, historyTable),
      partitionKey = partitionKey,
      granularity = granularity,
      historyTable = historyTable,
      outputDirectories = OUTPUT_DIRECTORY_KEYS.map(value(properties, _)).filter(_.nonEmpty).distinct,
      inputPaths = inputPaths(properties),
      confPath = confPath)
  }

  /**
   * Every `output.table.name.*` value, lowercased and de-duplicated, minus the history table.
   *
   * De-duplication matters: several keys can name the same table, and a table listed twice would be
   * counted twice in the report a person reads before approving.
   */
  private[engine] def outputTables(properties: Map[String, String], historyTable: String): Seq[String] = {
    val historyName = historyTable.split('.').lastOption.map(_.trim.toLowerCase).getOrElse("")
    properties
      .collect { case (key, v) if key.startsWith(TABLE_PREFIX) => v.trim.toLowerCase }
      .filter(_.nonEmpty)
      .filterNot(_ == historyName)
      .toSeq
      .distinct
      .sorted
  }

  /** Every `input.path.*` the run declares; blank values simply mean "feature not used by this run". */
  private[engine] def inputPaths(properties: Map[String, String]): Seq[String] =
    properties
      .collect { case (key, v) if key.startsWith(INPUT_PREFIX) => v.trim }
      .filter(_.nonEmpty)
      .toSeq
      .distinct
      .sorted

  private def value(properties: Map[String, String], key: String): String =
    properties.get(key).map(_.trim).getOrElse("")
}
