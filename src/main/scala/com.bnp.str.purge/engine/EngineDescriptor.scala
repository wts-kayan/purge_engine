package com.bnp.str.purge.engine

/**
 * How to read one engine's run configuration.
 *
 * Every STR engine is launched with a configuration file that already declares everything a purge
 * needs to know: where it wrote, under which name, and what it read. A descriptor is the small
 * amount of per-engine knowledge required to turn that file into an [[EngineRun]] — which key holds
 * the run id, which prefix marks an output table, what the run's partition key is called on disk.
 *
 * It is deliberately the ONLY place where an engine's conventions live. A purge that guessed table
 * names from a pattern, or assumed every engine partitions the same way, would be wrong the first
 * time an engine changed its layout — and wrong in the direction of deleting the wrong partition.
 */
trait EngineDescriptor {

  /** The name an operator writes in `purge_app.engine.name`. */
  def name: String

  /** The partition key this engine writes its run id under, in the ON-DISK spelling. */
  def partitionKey: String

  /**
   * What one run occupies, and therefore what deleting a run removes.
   *
   * Almost every engine stores a run as one PARTITION of each output table, so purging a run means
   * purging `runId=<uuid>` across those tables. The simulator is the exception: it stores a run as a
   * WHOLE TABLE, and purging a run there means dropping the tables themselves. Getting this wrong in
   * either direction is severe — deleting a whole table where a partition was meant, or leaving a
   * run behind where the table was.
   */
  def granularity: String = EngineDescriptor.GRANULARITY_PARTITION

  /** True when a run of this engine IS its tables rather than one partition of each. */
  final def isTableGranular: Boolean = granularity == EngineDescriptor.GRANULARITY_TABLE

  /**
   * The path segment that identifies a run of this engine on disk.
   *
   * PC04 looks for this token in a candidate's path to decide whether a job is still writing there,
   * and the run catalogue recovers a run id from it. The two granularities put the run's identity in
   * different places — a partition-granular engine writes `runId=<uuid>` inside each table, while a
   * table-granular one has no such directory at all and the TABLE is the run — so the token is
   * derived here, beside the granularity that decides it, rather than assumed by each caller.
   *
   * A table-granular engine whose tables are not named after the run id overrides this.
   */
  def runToken(runId: String): String =
    if (isTableGranular) runId else s"$partitionKey=$runId"

  /**
   * Read one run configuration into an [[EngineRun]].
   *
   * @param properties the parsed configuration file
   * @param confPath   where it was read from, carried through for the report and the audit
   */
  def read(properties: Map[String, String], confPath: String): EngineRun
}

object EngineDescriptor {

  /** A run is one partition of each output table (projection, and every engine but the simulator). */
  val GRANULARITY_PARTITION = "PARTITION"

  /** A run IS the table: purging the run drops the tables it wrote (the simulator). */
  val GRANULARITY_TABLE = "TABLE"

  /** The granularities this engine knows how to expand a run into. */
  val GRANULARITIES = Seq(GRANULARITY_PARTITION, GRANULARITY_TABLE)

  /** The engines this purge engine understands. Projection is the MVP; others follow the same shape. */
  val registry: Map[String, EngineDescriptor] =
    Seq(ProjectionEngine).map(descriptor => descriptor.name.toLowerCase -> descriptor).toMap

  /**
   * Resolve an engine by name, refusing an unknown one by listing what is supported.
   *
   * Fails rather than falling back to a generic reading of the file: a configuration this engine
   * cannot interpret must never be interpreted approximately, because the result of getting it
   * wrong is a purge aimed at the wrong partitions.
   */
  def of(name: String): EngineDescriptor = {
    val key = Option(name).map(_.trim.toLowerCase).getOrElse("")
    val descriptor = registry.getOrElse(key, throw new IllegalArgumentException(
      s"Unknown engine '$name'. Supported engines: ${registry.keys.toSeq.sorted.mkString(", ")}"))
    requireKnownGranularity(descriptor.name, descriptor.granularity)
    descriptor
  }

  /**
   * Refuse a granularity this engine does not implement, at the moment the descriptor is resolved.
   *
   * Granularity decides what deleting a run removes, so a value nobody handles must not travel any
   * further: everything downstream would fall through to the partition reading, which is the more
   * destructive of the two to get wrong and the one that fails silently — a scope of partition
   * directories that do not exist looks exactly like a run with nothing left to purge.
   */
  def requireKnownGranularity(engine: String, granularity: String): Unit =
    require(GRANULARITIES.contains(granularity),
      s"engine '$engine' declares granularity '$granularity'; supported: ${GRANULARITIES.mkString(", ")}")
}
