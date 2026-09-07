package com.bnp.str.purge.engine

/**
 * One execution of one STR engine, as its own run configuration describes it.
 *
 * This is the value that makes an engine-aware purge possible. Until now the engine reasoned about
 * *vintages* — an `as_of_date` partition ages out and may go. That is the right unit for a
 * date-partitioned output, and the wrong one for most of the engines on Promethee: they partition
 * by RUN. A projection run writes `runId=<uuid>` into thirty-odd tables at once, and those thirty
 * partitions are one thing — one execution, produced together, meaningful together, and purged
 * together or not at all. Purging "the 2023 partitions of term_structure" would leave the same
 * run's rows in twenty-nine sibling tables.
 *
 * So a purge request names a RUN, and the engine's own configuration file — the one it was launched
 * with, which TWIST already keeps — is what tells us what that run produced. Nothing is inferred
 * from naming conventions.
 *
 * @param engine            engine name, e.g. `projection`
 * @param runId             the run's identifier, and the value of its partition key on disk
 * @param runName           human name of the run, e.g. `IFRS9_scn26Q3_scenDraft`
 * @param runType           the run's business type, e.g. `IFRS9`
 * @param launchedBy        the user the engine recorded as having launched it
 * @param asOfDateQuarter   the run's as-of quarter, e.g. `2026Q3` — its business date
 * @param database          the Hive database the run wrote into
 * @param tables            every output table the run declares, LOWERCASED to match what Hive puts
 *                          on disk (the conf spells `projected_CR_detailed`, HDFS holds
 *                          `projected_cr_detailed`)
 * @param granularity       PARTITION when a run is one partition per output table, TABLE when the
 *                          run IS the table (the simulator) — it decides what deleting a run removes
 * @param historyTable      the engine's own run history, e.g. `dbprojection.run_history` — read to
 *                          find out whether the run is still going, never purged
 * @param outputDirectories non-Hive output the run wrote, e.g. its cluster output folder
 * @param inputPaths        the files the run READ. Shared across runs and never this run's to
 *                          delete — see control PC14.
 * @param confPath          where this description was read from, for the report and the audit
 */
final case class EngineRun(engine: String,
                           runId: String,
                           runName: String,
                           runType: String,
                           launchedBy: String,
                           asOfDateQuarter: String,
                           database: String,
                           tables: Seq[String],
                           partitionKey: String,
                           granularity: String,
                           historyTable: String,
                           outputDirectories: Seq[String],
                           inputPaths: Seq[String],
                           confPath: String) {

  EngineDescriptor.requireKnownGranularity(engine, granularity)

  /** True when this run IS its tables, rather than one partition of each of them. */
  def isTableGranular: Boolean = granularity == EngineDescriptor.GRANULARITY_TABLE

  /** `runId=9df8cf3a-…` — the partition directory name this run wrote, in the ON-DISK spelling. */
  def partitionDirectory: String = s"$partitionKey=$runId"

  /** `runid=9df8cf3a-…` — the same partition as the metastore spells it (Hive lowercases keys). */
  def partitionSpec: String = s"${partitionKey.toLowerCase}=$runId"

  /**
   * Where one of this run's output tables put the data, RELATIVE to the database location — and
   * therefore what a purge of this run removes from that table.
   *
   * This is the whole practical difference between the two granularities, and the reason
   * `granularity` cannot be a field nobody reads. A partition-granular run occupies
   * `term_structure/runId=<uuid>` and leaves the table itself alone; a table-granular one occupies
   * `term_structure` outright. Build the partition path for an engine that stores a run as a table
   * and the scope points at a directory that was never written: no error, no candidates, and a run
   * that reports success having purged nothing — the failure this codebase keeps having.
   */
  def relativePathOf(table: String): String =
    if (isTableGranular) table else s"$table/$partitionDirectory"

  /**
   * The metastore identity of what this run occupies in one table: its partition, or the table
   * itself. Empty for a table-granular run, which owns every partition of the table rather than one.
   */
  def partitionSpecOf: String = if (isTableGranular) "" else partitionSpec

  /** One line for the log and the report header. */
  def describe: String =
    s"$engine run $runId ('$runName', $runType, as-of $asOfDateQuarter, launched by $launchedBy): " +
      s"${tables.size} table(s) in $database, ${outputDirectories.size} output folder(s), " +
      s"granularity $granularity" +
      (if (isTableGranular) " (the run IS the table)" else s" ($partitionDirectory of each table)")
}
