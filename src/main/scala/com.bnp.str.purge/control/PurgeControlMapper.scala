package com.bnp.str.purge.control

import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import java.sql.Date

/** What the controls produced: the judged manifest, and the report that explains it. */
final case class ControlOutcome(manifest: DataFrame, report: CheckReport)

/**
 * Evaluates the thirteen controls over the manifest and fills the two columns P2 left empty:
 * `controls_ko` / `controls_warn`, and from them the `decision`.
 *
 * The mapper only ever JUDGES. It does not write, it does not delete, and it does not remove rows —
 * a blocked object stays in the manifest carrying the reason it was blocked, because the report has
 * to be able to say what was refused and why. Dropping those rows would produce a document that
 * only ever describes what will happen, which is the less useful half of a control report.
 *
 * Every rule is one boolean [[Column]] over the manifest, so the thirteen are evaluated in a single
 * pass and each one's definition sits in one readable place. The rules that need something the
 * frame does not carry — a live run, a dependency referential, an archive copy on the filesystem —
 * resolve that first, on the driver, and come back as a broadcast membership test.
 */
class PurgeControlMapper(checks: CheckConfig,
                         runHistory: => DataFrame,
                         protectedPaths: => Seq[String] = Seq.empty)(implicit sparkSession: SparkSession) {

  import PurgeControlMapper._

  private val log = LoggerFactory.getLogger(this.getClass)

  // By-name, materialised at most once: reading run_history costs a job, and a run whose controls
  // are switched off — or whose PC04 is — must not pay for it.
  private lazy val runHistoryFrame: DataFrame = runHistory

  private lazy val protectedPathList: Seq[String] = protectedPaths

  def apply(manifest: DataFrame,
            source: String,
            runId: String,
            requestId: String,
            manifestFingerprint: String): ControlOutcome = {

    if (!checks.enabled) {
      log.warn("controls.enabled = false -> no rule evaluated. The manifest keeps every object at " +
        s"${PrimaryConstants.DECISION_CANDIDATE}, which the executor never acts on: an unjudged " +
        "manifest can delete nothing.")
      return ControlOutcome(manifest, disabledReport(manifest, source, runId, requestId, manifestFingerprint))
    }

    // Materialised ONCE, here, before any rule touches it.
    //
    // Judging the manifest takes several actions over it — PC07 and PC09 each need one before the
    // rules can even be built, then the aggregate, then one small read per control that fired. The
    // manifest sits on top of the candidate-selection query, which is a chain of window functions;
    // recomputing it per action means re-running that query five or six times over the whole
    // inventory. Caching the caller's frame is not enough to rely on, because whether it happens to
    // be cached is the caller's business and the cost of getting it wrong lands here.
    val subject = manifest.persist()

    val evaluations = build(subject)
    val annotated = annotate(subject, evaluations)

    val report = summarise(annotated, evaluations, source, runId, requestId, manifestFingerprint)
    log.info(report.summaryLine)
    report.unknowns.foreach(r =>
      log.warn(s"[control] ${r.rule.id} ${r.rule.title}: UNKNOWN — ${r.action}"))

    // Projected back onto the manifest's own columns, in their original order: this drops the
    // temporary rule columns and keeps `purge_date` / `run_id` last, which is what the ORC write
    // partitions on and the convention the shared run_history follows.
    ControlOutcome(annotated.select(manifest.columns.map(col): _*), report)
  }

  // ---------------------------------------------------------------------------------------------
  // the rules
  // ---------------------------------------------------------------------------------------------

  private def build(manifest: DataFrame): Seq[RuleEval] = Seq(
    retentionNotReached,
    legalHold,
    pathOutOfScope,
    activeRun,
    downstreamDependency,
    minVersionsKept,
    noArchive(manifest),
    notAuthorized,
    blastRadius(manifest),
    sharedInput,
    noRetentionPolicy,
    recentlyAccessed,
    hiveMetadataOrphan(manifest),
    executionPhaseOnly(PurgeRule.ManifestDrift),
    executionPhaseOnly(PurgeRule.AlreadyAbsent)
  ).map(eval => if (checks.ruleEnabled(eval.rule)) eval else eval.disabled)

  /**
   * PC01 — a policy matched and its retention is not reached.
   *
   * An object NO policy matched does not fire here, only in PC15. The business owns its data and
   * decides what to purge, so what stands between the engine and an object is that someone named
   * it — a run scope drawn from the engine's own configuration — not that a policy authorised it.
   * `blockWithoutPolicy = true` restores the stricter reading for a perimeter that wants it.
   */
  private def retentionNotReached: RuleEval = {
    val rule = PurgeRule.RetentionNotReached
    val blockWithoutPolicy = checks.ruleBoolean(rule, "blockWithoutPolicy", default = false)

    val insideRetention =
      col("retention_cutoff").isNotNull &&
        (col("business_date").isNull || col("business_date") >= col("retention_cutoff"))

    RuleEval(rule,
      condition = Some(if (blockWithoutPolicy) insideRetention || col("policy_id").isNull else insideRetention),
      value = Some(
        when(col("policy_id").isNull, lit("no policy matched this path"))
          .otherwise(concat_ws(" ", lit("business date"), col("business_date").cast("string"),
            lit("is not before the cutoff"), col("retention_cutoff").cast("string")))))
  }

  /** PC15 — nothing but the explicit selection governs this object. Reported, never in the way. */
  private def noRetentionPolicy: RuleEval =
    RuleEval(PurgeRule.NoRetentionPolicy,
      condition = Some(col("policy_id").isNull),
      value = Some(lit("no policy in the retention referential matches this path")))

  /** PC02 — a regulatory hold on the matched policy. */
  private def legalHold: RuleEval =
    RuleEval(PurgeRule.LegalHold,
      condition = Some(coalesce(col("legal_hold"), lit(false)) === true),
      value = Some(concat_ws(" ", lit("policy"), col("policy_id"), lit("carries a legal hold"))))

  /**
   * PC03 — the path itself is not something this run may touch. The same
   * [[PrimaryUtilities.unsafePathReason]] the inventory used to refuse a scan root, so the reader,
   * the control and (at P4) PurgeGuard cannot disagree about what "out of scope" means.
   */
  private def pathOutOfScope: RuleEval = {
    val allowedRoots = checks.allowedRoots
    val reason = udf { path: String => PrimaryUtilities.unsafePathReason(path, allowedRoots).orNull }
    RuleEval(PurgeRule.PathOutOfScope,
      condition = Some(reason(col("path")).isNotNull),
      value = Some(reason(col("path"))))
  }

  /**
   * PC04 — a job is still writing here.
   *
   * An EMPTY run_history is reported UNKNOWN, never PASS. "No job is running" and "we have no idea
   * whether a job is running" are different facts, and only one of them is a clearance.
   */
  private def activeRun: RuleEval = {
    val rule = PurgeRule.ActiveRun
    val quietMinutes = checks.ruleInt(rule, "minQuietMinutes", DEFAULT_QUIET_MINUTES)

    if (runHistoryFrame.take(1).isEmpty) return RuleEval(rule).noData

    val maxRunHours = checks.ruleInt(rule, "maxRunHours", DEFAULT_MAX_RUN_HOURS)
    val busy = busyTokens(runHistoryFrame, quietMinutes, maxRunHours)
    log.info(s"[control] ${rule.id}: ${busy.size} table(s) busy in run_history " +
      s"(not finished, or finished within $quietMinutes minutes)")

    val isBusy = udf { (table: String, path: String) => touchesAny(busy, table, path) }
    RuleEval(rule,
      condition = Some(isBusy(coalesce(col("table_name"), lit("")), col("path"))),
      value = Some(lit(s"a job has not finished here, or finished within $quietMinutes minutes")))
  }

  /** PC05 — a declared consumer still reads this table. No referential means UNKNOWN, not PASS. */
  private def downstreamDependency: RuleEval = {
    val rule = PurgeRule.DownstreamDependency
    val referential = checks.ruleString(rule, "referential", "")
    if (referential.isEmpty) return RuleEval(rule).noData

    val consumed =
      try readDependencies(referential)
      catch {
        case e: Throwable =>
          log.warn(s"[control] ${rule.id}: could not read the dependency referential '$referential' " +
            s"(${e.getClass.getSimpleName}: ${e.getMessage})")
          return RuleEval(rule).noData
      }

    log.info(s"[control] ${rule.id}: ${consumed.size} table(s) declared as feeding a downstream object")
    val isConsumed = udf { (database: String, table: String) => consumed.contains((database, table)) }
    RuleEval(rule,
      condition = Some(isConsumed(coalesce(col("database_name"), lit("")), coalesce(col("table_name"), lit("")))),
      value = Some(lit(s"declared in $referential as feeding a downstream object")))
  }

  /** PC06 — the newest `keepMinVersions` vintages of a table survive whatever their age. */
  private def minVersionsKept: RuleEval =
    RuleEval(PurgeRule.MinVersionsKept,
      condition = Some(coalesce(col("keep_min_versions"), lit(0)) > 0 &&
        col("version_rank") <= coalesce(col("keep_min_versions"), lit(0))),
      value = Some(concat_ws(" ", lit("vintage"), col("version_rank"), lit("of"),
        col("version_group_total"), lit("- the policy keeps the newest"),
        col("keep_min_versions"))))

  /**
   * PC07 — an unrecoverable delete without the archive its policy demands.
   *
   * Existence is checked on the driver, once per candidate concerned: only a HARD strategy with
   * `archiveRequired` reaches the check, which is a deliberate and rare combination. The archive is
   * looked for at `archiveRoot` + the object's path relative to its scan root — the convention this
   * engine assumes until the archive tier is settled (specification §15, open question 6).
   */
  private def noArchive(manifest: DataFrame): RuleEval = {
    val rule = PurgeRule.NoArchive
    if (!checks.ruleEnabled(rule)) return RuleEval(rule).disabled

    val concerned = manifest
      .where(coalesce(col("archive_required"), lit(false)) === true &&
        upper(coalesce(col("strategy"), lit(""))) === PrimaryConstants.STRATEGY_HARD)
      .select("path", "root_path", "archive_root")
      .distinct()
      .limit(MAX_ARCHIVE_CHECKS + 1)
      .collect()

    if (concerned.isEmpty) return RuleEval(rule, condition = Some(lit(false)))

    if (concerned.length > MAX_ARCHIVE_CHECKS) {
      log.warn(s"[control] ${rule.id}: more than $MAX_ARCHIVE_CHECKS objects require an archive " +
        "check; refusing to issue that many filesystem calls and reporting UNKNOWN instead")
      return RuleEval(rule).noData
    }

    val missing = concerned.filterNot(row => archiveExists(row)).map(_.getString(0)).toSet
    log.info(s"[control] ${rule.id}: ${concerned.length} object(s) require an archive, " +
      s"${missing.size} without one")

    val isMissing = udf { path: String => missing.contains(path) }
    RuleEval(rule,
      condition = Some(isMissing(col("path"))),
      value = Some(concat_ws(" ", lit("no archive copy under"), col("archive_root"))))
  }

  /** PC08 — the requester does not own this perimeter. No group list means UNKNOWN, not PASS. */
  private def notAuthorized: RuleEval = {
    val rule = PurgeRule.NotAuthorized
    if (checks.requesterGroups.isEmpty) return RuleEval(rule).noData

    val groups = checks.requesterGroups
    RuleEval(rule,
      condition = Some(coalesce(col("owner_group"), lit("")) =!= "" &&
        !col("owner_group").isin(groups: _*)),
      value = Some(concat_ws(" ", lit(s"'${checks.requester}' is not in"), col("owner_group"))))
  }

  /**
   * PC09 — the run as a whole is too large.
   *
   * Run-level, not object-level: every candidate is blocked when a total ceiling is crossed, and
   * only the offending tables are blocked when the per-table share is. A purge that is defensible
   * object by object can still be a mistake taken together, and that is precisely the mistake an
   * approver is least able to catch by reading a list.
   *
   * **The per-table share does not apply to an object that IS a whole table.** `maxPercentOfTable`
   * asks "would this gut a partitioned table?", and for a table-granular run — the classic
   * simulator, where a run IS its tables — the answer is 100% by construction: the table holds
   * exactly one object, itself. Left in, the ceiling would block every simulator purge that was ever
   * attempted, for a reason that reads like a safety margin and is really an arithmetic accident.
   * The ceilings that DO apply to such a run are the ones on total bytes and total objects, and they
   * are unchanged.
   */
  private def blastRadius(manifest: DataFrame): RuleEval = {
    val rule = PurgeRule.BlastRadius

    val totals = manifest.agg(
      count(lit(1)).as("objects"),
      coalesce(sum(col("size_bytes")), lit(0L)).as("bytes")).head()

    val objects = totals.getAs[Long]("objects")
    val bytes = totals.getAs[Long]("bytes")

    if (objects == 0L) return RuleEval(rule, condition = Some(lit(false)))

    val tooManyObjects = objects > checks.maxObjects
    val tooManyBytes = bytes > checks.maxBytes

    // Whole tables are excluded from the share before it is computed, not blocked and then excused.
    val partitionsOnly =
      if (manifest.columns.contains("is_registered_table"))
        manifest.where(!coalesce(col("is_registered_table"), lit(false)))
      else manifest

    val overShare = partitionsOnly
      .groupBy(col("version_group"))
      .agg(count(lit(1)).as("selected"), max(coalesce(col("version_group_total"), lit(0))).as("total"))
      .where(col("total") > 0 && (col("selected") * 100.0 / col("total")) > checks.maxPercentOfTable)
      .select("version_group")
      .collect()
      .map(_.getString(0))
      .toSet

    if (tooManyObjects || tooManyBytes)
      log.warn(s"[control] ${rule.id}: the run exceeds a ceiling — $objects object(s) " +
        s"(max ${checks.maxObjects}), ${PrimaryUtilities.humanBytes(bytes)} " +
        s"(max ${PrimaryUtilities.humanBytes(checks.maxBytes)}); every candidate is blocked")
    if (overShare.nonEmpty)
      log.warn(s"[control] ${rule.id}: ${overShare.size} table(s) would lose more than " +
        s"${checks.maxPercentOfTable}% of their vintages")

    val runLevel = tooManyObjects || tooManyBytes
    val reasonText =
      if (runLevel) s"the run exceeds a ceiling: $objects object(s), ${PrimaryUtilities.humanBytes(bytes)}"
      else s"more than ${checks.maxPercentOfTable}% of this table's vintages"

    val wholeTable =
      if (manifest.columns.contains("is_registered_table")) coalesce(col("is_registered_table"), lit(false))
      else lit(false)

    RuleEval(rule,
      condition = Some(
        if (runLevel) lit(true)
        else if (overShare.isEmpty) lit(false)
        else col("version_group").isin(overShare.toSeq: _*) && !wholeTable),
      value = Some(lit(reasonText)))
  }

  /**
   * PC14 — the object is a declared input of a run being purged, not its output.
   *
   * Containment is checked in BOTH directions: the candidate may BE an input file, or it may be a
   * directory that holds one. Deleting the folder a shared model sits in destroys it just as
   * thoroughly as deleting the file.
   */
  private def sharedInput: RuleEval = {
    val rule = PurgeRule.SharedInput
    val protectedList = protectedPathList
    if (protectedList.isEmpty) return RuleEval(rule, condition = Some(lit(false)))

    val conflicting = udf { path: String =>
      protectedList.find(p =>
        PrimaryUtilities.isUnderRoot(path, p) || PrimaryUtilities.isUnderRoot(p, path)).orNull
    }
    RuleEval(rule,
      condition = Some(conflicting(col("path")).isNotNull),
      value = Some(concat_ws(" ", lit("shared with"), conflicting(col("path")))))
  }

  /** PC10 — someone may still be reading it. A warning: atime is not reliable enough to veto on. */
  private def recentlyAccessed: RuleEval = {
    val rule = PurgeRule.RecentlyAccessed
    val days = checks.ruleInt(rule, "recentAccessDays", DEFAULT_RECENT_ACCESS_DAYS)
    val reference = lit(Date.valueOf(checks.asOfDate))

    RuleEval(rule,
      condition = Some(col("access_time").isNotNull && datediff(reference, col("access_time")) <= days),
      value = Some(concat_ws(" ", lit("last read"), col("access_time").cast("string"))))
  }

  /**
   * PC11 — deleting the files alone would orphan the metastore entry.
   *
   * Fires for a registered partition and for a whole registered table alike: a table-granular run
   * leaves an unreadable table behind exactly as a partition-granular one leaves an unreadable
   * partition. The executor is instructed to drop whichever it is.
   */
  private def hiveMetadataOrphan(manifest: DataFrame): RuleEval =
    if (manifest.columns.contains("is_registered_table")) RuleEval(PurgeRule.HiveMetadataOrphan,
      condition = Some(coalesce(col("is_registered_partition"), lit(false)) === true ||
        coalesce(col("is_registered_table"), lit(false)) === true),
      value = Some(lit("the metastore still holds this object; the executor drops it as well")))
    else hiveMetadataOrphanPartitionOnly

  private def hiveMetadataOrphanPartitionOnly: RuleEval =
    RuleEval(PurgeRule.HiveMetadataOrphan,
      condition = Some(coalesce(col("is_registered_partition"), lit(false)) === true),
      value = Some(concat_ws(" ", lit("registered as"), col("database_name"), lit("."),
        col("table_name"), col("partition_spec"))))

  /** PC12 / PC13 — meaningful only once there is a manifest to have drifted from. */
  private def executionPhaseOnly(rule: PurgeRule): RuleEval =
    RuleEval(rule, notEvaluated = Some(NotEvaluated.EXECUTE_PHASE))

  // ---------------------------------------------------------------------------------------------
  // annotation and reporting
  // ---------------------------------------------------------------------------------------------

  /**
   * One boolean column per evaluated rule, then the two arrays and the decision derived from them.
   *
   * Three `select`s rather than fourteen chained `withColumn` calls, for the same reason as
   * [[com.bnp.str.purge.mapping.ManifestView.build]]: each `withColumn` re-analyses the entire plan,
   * and the plan here is the manifest on top of the selection query. Chained, the thirteen controls
   * cost minutes of planning on a frame that takes milliseconds to evaluate.
   */
  private def annotate(manifest: DataFrame, evaluations: Seq[RuleEval]): DataFrame = {
    val evaluated = evaluations.filter(_.condition.isDefined)

    // the manifest already carries the three columns P2 left empty; they are replaced, not added
    val base = manifest.drop(JUDGEMENT_COLUMNS: _*)

    val flags = evaluated.map(eval => coalesce(eval.condition.get, lit(false)).as(eval.columnName.get))
    val withFlags = base.select(col("*") +: flags: _*)

    val withArrays = withFlags.select(
      col("*"),
      idsOf(evaluated.filter(_.rule.blocking)).as("controls_ko"),
      idsOf(evaluated.filterNot(_.rule.blocking)).as("controls_warn"))

    withArrays.select(
      col("*"),
      when(size(col("controls_ko")) > 0, lit(PrimaryConstants.DECISION_BLOCKED))
        .when(size(col("controls_warn")) > 0, lit(PrimaryConstants.DECISION_WARN))
        .otherwise(lit(PrimaryConstants.DECISION_DELETE)).as("decision"))
  }

  /** The ids of the rules that fired on a row, as an `array<string>`. */
  private def idsOf(evaluations: Seq[RuleEval]): Column =
    evaluations
      .map(eval => when(col(eval.columnName.get), array(lit(eval.rule.id))).otherwise(EMPTY_IDS))
      .reduceOption((a, b) => concat(a, b))
      .getOrElse(EMPTY_IDS)

  /** Counts, bytes and capped findings — the aggregate in one pass, then one small read per rule. */
  private def summarise(annotated: DataFrame,
                        evaluations: Seq[RuleEval],
                        source: String,
                        runId: String,
                        requestId: String,
                        manifestFingerprint: String): CheckReport = {

    val evaluated = evaluations.filter(_.condition.isDefined)

    val aggregates =
      Seq(count(lit(1)).as("candidates"),
        coalesce(sum(col("size_bytes")), lit(0L)).as("bytes_total"),
        coalesce(sum(when(col("decision") === PrimaryConstants.DECISION_DELETE ||
          col("decision") === PrimaryConstants.DECISION_WARN, col("size_bytes"))), lit(0L)).as("bytes_free"),
        coalesce(sum(when(col("decision") === PrimaryConstants.DECISION_DELETE, 1L)), lit(0L)).as("to_delete"),
        coalesce(sum(when(col("decision") === PrimaryConstants.DECISION_WARN, 1L)), lit(0L)).as("to_warn"),
        coalesce(sum(when(col("decision") === PrimaryConstants.DECISION_BLOCKED, 1L)), lit(0L)).as("blocked")) ++
        evaluated.flatMap { eval =>
          Seq(
            coalesce(sum(when(col(eval.columnName.get), 1L)), lit(0L)).as(s"n_${eval.rule.id}"),
            coalesce(sum(when(col(eval.columnName.get), col("size_bytes"))), lit(0L)).as(s"b_${eval.rule.id}"))
        }

    val row = annotated.agg(aggregates.head, aggregates.tail: _*).head()

    val results = evaluations.map { eval =>
      val total = if (eval.condition.isDefined) row.getAs[Long](s"n_${eval.rule.id}") else 0L
      CheckRuleResult(
        rule = eval.rule,
        enabled = eval.enabled,
        notEvaluated = eval.notEvaluated,
        total = total,
        findings = if (total > 0L) findingsOf(annotated, eval) else Seq.empty,
        bytes = if (eval.condition.isDefined) row.getAs[Long](s"b_${eval.rule.id}") else 0L)
    }

    CheckReport(
      source = source,
      runId = runId,
      requestId = requestId,
      generatedAt = java.time.LocalDateTime.now().withNano(0).toString.replace('T', ' '),
      asOfDate = checks.asOfDate.toString,
      manifestFingerprint = manifestFingerprint,
      candidates = row.getAs[Long]("candidates"),
      toDelete = row.getAs[Long]("to_delete"),
      toWarn = row.getAs[Long]("to_warn"),
      blocked = row.getAs[Long]("blocked"),
      bytesTotal = row.getAs[Long]("bytes_total"),
      bytesToFree = row.getAs[Long]("bytes_free"),
      results = results)
  }

  private def findingsOf(annotated: DataFrame, eval: RuleEval): Seq[PurgeFinding] =
    annotated
      .where(col(eval.columnName.get))
      .select(
        col("path"),
        coalesce(col("version_group"), lit("")).as("table"),
        coalesce(eval.value.getOrElse(lit("")).cast("string"), lit("")).as("value"),
        coalesce(col("size_bytes"), lit(0L)).as("size_bytes"))
      .orderBy(col("size_bytes").desc)
      .limit(checks.maxFindingsPerRule)
      .collect()
      .map(row => PurgeFinding(row.getString(0), row.getString(1), row.getString(2), row.getLong(3)))
      .toList

  /** The report of a run whose controls were switched off: everything SKIPPED, nothing deletable. */
  private def disabledReport(manifest: DataFrame,
                             source: String,
                             runId: String,
                             requestId: String,
                             manifestFingerprint: String): CheckReport = {
    val row = manifest.agg(
      count(lit(1)).as("candidates"),
      coalesce(sum(col("size_bytes")), lit(0L)).as("bytes")).head()

    CheckReport(
      source = source,
      runId = runId,
      requestId = requestId,
      generatedAt = java.time.LocalDateTime.now().withNano(0).toString.replace('T', ' '),
      asOfDate = checks.asOfDate.toString,
      manifestFingerprint = manifestFingerprint,
      candidates = row.getAs[Long]("candidates"),
      toDelete = 0L,
      toWarn = 0L,
      blocked = 0L,
      bytesTotal = row.getAs[Long]("bytes"),
      bytesToFree = 0L,
      results = PurgeRule.All.map(rule =>
        CheckRuleResult(rule, enabled = false, notEvaluated = Some(NotEvaluated.DISABLED),
          total = 0L, findings = Seq.empty, bytes = 0L)))
  }

  /** Existence of the archive copy of one candidate, under the convention documented on PC07. */
  private def archiveExists(row: Row): Boolean = {
    val path = row.getString(0)
    val rootPath = Option(row.getString(1)).getOrElse("")
    val archiveRoot = Option(row.getString(2)).getOrElse("")
    if (archiveRoot.isEmpty) return false

    val relative = if (rootPath.nonEmpty && path.startsWith(rootPath)) path.drop(rootPath.length) else path
    val candidate = new Path(s"${archiveRoot.stripSuffix("/")}/${relative.stripPrefix("/")}")
    try candidate.getFileSystem(sparkSession.sparkContext.hadoopConfiguration).exists(candidate)
    catch { case _: Throwable => false }
  }

  /** The `(database, table)` pairs the dependency referential declares as feeding something else. */
  private def readDependencies(referential: String): Set[(String, String)] = {
    val table = sparkSession.table(referential)
    val columns = table.columns.toSet
    val databaseColumn = Seq("source_database", "database_name", "database").find(columns.contains)
    val tableColumn = Seq("source_table", "table_name", "table").find(columns.contains)

    require(tableColumn.isDefined,
      s"the dependency referential '$referential' has no source_table / table_name column")

    table
      .select(
        coalesce(databaseColumn.map(col).getOrElse(lit("")), lit("")).as("db"),
        coalesce(col(tableColumn.get), lit("")).as("tbl"))
      .distinct()
      .collect()
      .map(row => (row.getString(0), row.getString(1)))
      .toSet
  }
}

object PurgeControlMapper {

  /** The three columns P2 writes empty and the controls fill in. */
  val JUDGEMENT_COLUMNS = Seq("controls_ko", "controls_warn", "decision")

  val DEFAULT_QUIET_MINUTES = 60

  /**
   * Beyond this, a run_history row with no end date is presumed dead rather than still running.
   *
   * Nothing writes an end date for a job whose JVM was killed, so without a bound those rows would
   * protect their run for ever — and the runs nobody finished are exactly the ones people want to
   * purge.
   */
  val DEFAULT_MAX_RUN_HOURS = 24

  /**
   * Statuses that mean a run is over, lower-cased. Used only when a history table carries no
   * end_date column; `dbprojection.run_history` does carry one, and the absence of an end is a far
   * better signal than any word.
   */
  val TERMINAL_STATUSES: Set[String] =
    Set("succeeded", "success", "successful", "failed", "failure", "error", "killed", "aborted",
      "cancelled", "canceled", "finished", "completed", "done")
  val DEFAULT_RECENT_ACCESS_DAYS = 30

  /**
   * Above this many objects needing an archive check, PC07 reports UNKNOWN instead of issuing one
   * NameNode call per object. A control must not be the reason a simulation takes an hour — and an
   * honest "not checked" is worth more than a check that never finishes.
   */
  val MAX_ARCHIVE_CHECKS = 5000

  private val EMPTY_IDS: Column = array().cast("array<string>")

  /**
   * One rule, ready to evaluate.
   *
   * `condition` is None exactly when the rule produced no verdict — disabled, execution-phase, or
   * missing input — and `notEvaluated` then says which of the three it was.
   */
  private[control] final case class RuleEval(rule: PurgeRule,
                                             enabled: Boolean = true,
                                             notEvaluated: Option[String] = None,
                                             condition: Option[Column] = None,
                                             value: Option[Column] = None) {

    /** Temporary column holding this rule's verdict per row; dropped before the manifest is returned. */
    def columnName: Option[String] = condition.map(_ => s"__ctl_${rule.id}")

    def disabled: RuleEval = copy(enabled = false, notEvaluated = Some(NotEvaluated.DISABLED), condition = None)

    def noData: RuleEval = copy(notEvaluated = Some(NotEvaluated.NO_DATA), condition = None)
  }

  /**
   * Tables a job is writing to right now: every run_history row still RUNNING, plus those that
   * finished within the quiet period, named by `base_folder_name` — the column each module fills
   * with what it produced.
   *
   * Column-by-column defensive reads: `run_history` is shared with every other engine and gains
   * columns over time, and a purge must not fail because a sibling module added one.
   */
  private[control] def busyTokens(runHistory: DataFrame,
                                  quietMinutes: Int,
                                  maxRunHours: Int = DEFAULT_MAX_RUN_HOURS): Set[String] = {
    val columns = runHistory.columns.toSet
    if (!columns.contains("base_folder_name")) return Set.empty

    val hasEndDate = columns.contains("end_date")
    val hasStart = columns.contains("creation_date")

    // A run with no END is a run that has not finished — whatever word its engine uses for
    // "running". `dbprojection.run_history` stores `succeeded` in lower case and nothing documents
    // its in-flight value, so matching a status literal is a guess, and a wrong guess here means the
    // one control that stops a purge deleting under a live writer never fires at all.
    val unfinished =
      if (!hasEndDate) lit(false)
      else if (hasStart)
        // ...bounded, because a row left unfinished by a killed JVM would otherwise protect its run
        // for ever. Past `maxRunHours` the row is presumed dead rather than running.
        col("end_date").isNull &&
          col("creation_date").isNotNull &&
          col("creation_date") >= (current_timestamp() - expr(s"INTERVAL $maxRunHours HOURS"))
      else col("end_date").isNull

    // Belt and braces for a history table that carries no end_date at all.
    val runningStatus =
      if (columns.contains("status"))
        !lower(coalesce(col("status"), lit(""))).isin(TERMINAL_STATUSES.toSeq: _*)
      else lit(false)

    val recentlyFinished =
      if (hasEndDate)
        col("end_date").isNotNull &&
          col("end_date") >= (current_timestamp() - expr(s"INTERVAL $quietMinutes MINUTES"))
      else lit(false)

    val busy = if (hasEndDate) unfinished || recentlyFinished else runningStatus

    runHistory
      .where(busy)
      .select(coalesce(col("base_folder_name"), lit("")).as("token"))
      .distinct()
      .collect()
      .map(_.getString(0))
      .filter(_.nonEmpty)
      .toSet
  }

  /** True when one of the busy tables IS this candidate's table, or names a folder on its path. */
  private[control] def touchesAny(busy: Set[String], table: String, path: String): Boolean = {
    val safePath = Option(path).getOrElse("")
    val safeTable = Option(table).getOrElse("")
    busy.exists(token =>
      (safeTable.nonEmpty && token.equalsIgnoreCase(safeTable)) ||
        safePath.contains(s"/$token/") ||
        safePath.endsWith(s"/$token"))
  }
}
