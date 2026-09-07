package com.bnp.str.purge.purge

import com.bnp.str.purge.engine.EngineDescriptor
import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.time.{Duration, LocalDateTime}

/**
 * The last gate before the first deletion, and the only one that cannot be switched off.
 *
 * Everything it checks is checked again by a control — PC03, PC09, PC12 — and that duplication is
 * the point. Controls are configuration; configuration is exactly what may be wrong, and the run
 * this protects against is the one where someone disabled a rule they did not understand. So the
 * guard re-derives the dangerous conditions from the manifest in code, with no conf to consult
 * beyond the ceilings themselves, and refuses **the whole run** rather than the offending row: a
 * manifest containing one impossible target is not a manifest to partially trust.
 *
 * It throws. There is no "guard failed but continue".
 */
object PurgeGuard {

  private val log = LoggerFactory.getLogger(this.getClass)

  /** Refuse the run unless every target in the manifest is one this engine may delete. */
  def assertSafe(manifest: DataFrame)(implicit sparkSession: SparkSession, conf: Config): Unit = {
    val guardConfig = configOf(conf, PrimaryConstants.GUARD)
    val allowedRoots = PrimaryUtilities
      .getStringList(conf.getConfig(s"${PrimaryConstants.APP_CONF}.${PrimaryConstants.SCAN}"),
        PrimaryConstants.SCAN_ALLOWED_ROOTS)
      .map(PrimaryUtilities.qualifyPath)

    val deletable = manifest.where(col("decision").isin(PrimaryConstants.DELETABLE_DECISIONS: _*))

    val refusals =
      unsafeTargets(deletable, allowedRoots) ++
        overCeilings(deletable, guardConfig) ++
        staleManifest(manifest, guardConfig) ++
        missingApproval(manifest, conf, guardConfig) ++
        unsupportedGranularity(engineDescriptor(conf))

    if (refusals.nonEmpty)
      throw new IllegalStateException(
        s"PurgeGuard refuses this run; ${refusals.size} reason(s):\n" +
          refusals.map(reason => s"  - $reason").mkString("\n"))

    val objects = deletable.count()
    log.info(s"PurgeGuard passed: $objects object(s) may be deleted, every one of them inside " +
      s"[${allowedRoots.mkString(", ")}]")
  }

  /**
   * Targets this engine may never touch, whatever the manifest says: outside the allowed roots, too
   * shallow, on the forbidden list, or still carrying a traversal or a wildcard.
   *
   * The same [[PrimaryUtilities.unsafePathReason]] the inventory used to refuse a scan root and PC03
   * used to block a candidate — one definition of "out of scope", checked at all three points.
   */
  private def unsafeTargets(deletable: DataFrame, allowedRoots: Seq[String])
                           (implicit sparkSession: SparkSession): Seq[String] = {
    val reason = udf { path: String => PrimaryUtilities.unsafePathReason(path, allowedRoots).orNull }

    deletable
      .select(col("path"), reason(col("path")).as("reason"))
      .where(col("reason").isNotNull)
      .limit(MAX_REPORTED)
      .collect()
      .map(row => s"unsafe target: ${row.getString(1)}")
      .toList
  }

  /** Total bytes and total objects, re-derived from the manifest rather than trusted from a report. */
  private def overCeilings(deletable: DataFrame, guardConfig: Config): Seq[String] = {
    val totals = deletable.agg(
      count(lit(1)).as("objects"),
      coalesce(sum(col("size_bytes")), lit(0L)).as("bytes")).head()

    val objects = totals.getAs[Long]("objects")
    val bytes = totals.getAs[Long]("bytes")
    val maxObjects = getLong(guardConfig, "maxObjects", Long.MaxValue)
    val maxBytes = getLong(guardConfig, "maxBytes", Long.MaxValue)

    Seq(
      if (objects > maxObjects) Some(s"$objects object(s) exceeds the ceiling of $maxObjects") else None,
      if (bytes > maxBytes)
        Some(s"${PrimaryUtilities.humanBytes(bytes)} exceeds the ceiling of " +
          PrimaryUtilities.humanBytes(maxBytes))
      else None
    ).flatten
  }

  /**
   * A manifest older than `guard.manifestMaxAgeHours` may not be executed.
   *
   * The filesystem moves. A manifest describes what was true when it was built, and past some age
   * that description is a guess — the per-object drift check catches what changed, but not what was
   * created since, which no longer appears anywhere.
   */
  private def staleManifest(manifest: DataFrame, guardConfig: Config): Seq[String] = {
    val maxAgeHours = getLong(guardConfig, "manifestMaxAgeHours", DEFAULT_MANIFEST_MAX_AGE_HOURS)
    if (maxAgeHours <= 0L) return Seq.empty

    val generated = manifest.select(col("generated_at")).limit(1).collect().headOption
      .flatMap(row => Option(row.getAs[java.sql.Timestamp]("generated_at")))

    generated.toSeq.flatMap { timestamp =>
      val age = Duration.between(timestamp.toLocalDateTime, LocalDateTime.now()).toHours
      if (age > maxAgeHours)
        Seq(s"the manifest was generated $age hour(s) ago, past the limit of $maxAgeHours; " +
          "re-run the simulation")
      else Seq.empty
    }
  }

  /**
   * The approval token, when a perimeter still wants one.
   *
   * The business decided against an approval step for this version (Q11), so `requireApproval`
   * defaults to false and this check is normally inert. It stays implemented because switching it
   * back on must be a configuration change and not a code change: when a perimeter does want a
   * second signature, the token has to be verified against the fingerprint of the manifest actually
   * being executed, which is the one thing an approval is about.
   */
  private def missingApproval(manifest: DataFrame, conf: Config, guardConfig: Config): Seq[String] = {
    if (!getBoolean(guardConfig, "requireApproval", default = false)) return Seq.empty

    val request = configOf(conf, PrimaryConstants.REQUEST)
    val token = PrimaryUtilities.getStringOr(request, "approvalToken", "")
    val approver = PrimaryUtilities.getStringOr(request, "approver", "")
    val requester = PrimaryUtilities.getStringOr(request, "requester", "")

    val fingerprint = manifest.select(col("manifest_fingerprint")).limit(1).collect().headOption
      .map(_.getString(0)).getOrElse("")

    Seq(
      if (token.isEmpty) Some("guard.requireApproval is on but request.approvalToken is empty") else None,
      if (approver.isEmpty) Some("guard.requireApproval is on but request.approver is empty") else None,
      if (approver.nonEmpty && approver == requester)
        Some(s"the approver and the requester are the same person ('$approver')") else None,
      if (token.nonEmpty && fingerprint.nonEmpty && !token.contains(fingerprint.take(TOKEN_FINGERPRINT_CHARS)))
        Some("the approval token does not carry the fingerprint of this manifest") else None
    ).flatten
  }

  /**
   * Refuse to EXECUTE a run of an engine whose granularity the executor cannot carry out.
   *
   * A table-granular run (the simulator, per Q1) IS its tables, so purging it means the tables
   * themselves go. [[PurgeExecutor.maybeDropPartition]] only ever drops a PARTITION, and skips a row
   * whose `partition_spec` is empty — which is every row of a table-granular scope. The run would
   * therefore trash the data and leave the table registered in the metastore, pointing at a location
   * that no longer holds anything: a half-done deletion that reports SUCCESS, and the exact shape of
   * failure §16.Q1 calls severe.
   *
   * So the simulation of such a run is allowed — seeing what would go is useful and harms nothing —
   * and the deletion is refused until the executor learns to drop a table. Refusing here rather than
   * in the controls is deliberate: this is a limit of the code, not a policy someone may switch off.
   */
  // `private[str]`, not `private[purge]`: inside com.bnp.str.purge.purge the latter names THIS
  // package, which the specs in com.bnp.str.purge cannot reach.
  private[str] def unsupportedGranularity(descriptor: Option[EngineDescriptor]): Seq[String] =
    descriptor.filter(_.isTableGranular).toSeq.map(d =>
      s"engine '${d.name}' declares granularity ${EngineDescriptor.GRANULARITY_TABLE}, where a run " +
        "IS its tables; the executor can only drop a partition, so executing this manifest would " +
        "remove the data and leave the table registered. Simulation is available; execution is not " +
        "until PurgeExecutor implements DROP TABLE.")

  /** The engine this run is about, when one is configured. `None` for a path- or policy-driven run. */
  private def engineDescriptor(conf: Config): Option[EngineDescriptor] = {
    val engineConfig = configOf(conf, PrimaryConstants.ENGINE)
    Some(PrimaryUtilities.getStringOr(engineConfig, "name", "").trim)
      .filter(_.nonEmpty)
      .map(EngineDescriptor.of)
  }

  // ---------------------------------------------------------------------------------------------

  /** How many unsafe targets to name before the message stops being readable. */
  private val MAX_REPORTED = 20

  private val DEFAULT_MANIFEST_MAX_AGE_HOURS = 72L

  /** The token is expected to carry at least this much of the fingerprint it was computed over. */
  private val TOKEN_FINGERPRINT_CHARS = 16

  private def configOf(conf: Config, block: String): Config = {
    val path = s"${PrimaryConstants.APP_CONF}.$block"
    if (conf.hasPath(path)) conf.getConfig(path) else ConfigFactory.empty()
  }

  private def getLong(cfg: Config, key: String, default: Long): Long =
    if (cfg.hasPath(key)) cfg.getLong(key) else default

  private def getBoolean(cfg: Config, key: String, default: Boolean): Boolean =
    if (cfg.hasPath(key)) cfg.getBoolean(key) else default
}
