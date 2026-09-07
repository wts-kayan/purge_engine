package com.bnp.str.purge.control

import com.bnp.str.purge.utility.PrimaryUtilities

/**
 * Value model of the purge control report.
 *
 * Pure data — no Spark, no IO — so the rules ([[PurgeControlMapper]]) and the rendering
 * ([[CheckHtmlView]]) can be unit-tested independently of each other.
 */

/**
 * One control.
 *
 * @param id       stable identifier used in the report, in the manifest's `controls_ko` column, and
 *                 in the logs (PC01, PC02, ...)
 * @param key      its config key under `purge_app.controls { }`
 * @param title    short human label
 * @param detail   what the control checks, in the business team's own terms
 * @param blocking true when a hit REMOVES the object from what may be deleted. A blocking control is
 *                 not advice: no approval overrides it, because the thing it protects against is a
 *                 person approving something they did not fully understand.
 * @param locked   true for the three controls that cannot be switched off, because `PurgeGuard`
 *                 re-checks them in code at P4. The configuration is exactly what may be wrong.
 */
final case class PurgeRule(id: String,
                           key: String,
                           title: String,
                           detail: String,
                           blocking: Boolean,
                           locked: Boolean = false)

object PurgeRule {

  val RetentionNotReached = PurgeRule(
    id = "PC01",
    key = "retention_not_reached",
    title = "Retention not reached",
    detail = "A retention policy matched this object and its business date is not yet older than " +
      "that retention. An object no policy matched is NOT blocked here (see PC15): the business owns " +
      "its data and decides what to purge, so the deny boundary is 'it must have been explicitly " +
      "named' rather than 'a policy must authorise it' — and naming is exactly what a run scope is. " +
      "Set `retention_not_reached.blockWithoutPolicy = true` to restore the stricter reading, where " +
      "nothing may be deleted unless a policy authorises it.",
    blocking = true)

  val LegalHold = PurgeRule(
    id = "PC02",
    key = "legal_hold",
    title = "Legal hold",
    detail = "The matched policy carries a legal or regulatory hold. Regulatory retention is not a " +
      "preference that a purge request can outweigh, so the object is blocked regardless of its age " +
      "and regardless of who approved the request.",
    blocking = true)

  val PathOutOfScope = PurgeRule(
    id = "PC03",
    key = "path_out_of_scope",
    title = "Path out of scope",
    detail = "The path is outside `scan.allowedRoots`, is shallower than the minimum depth, sits on " +
      "the forbidden list, or still contains a traversal or a wildcard. Cannot be switched off: " +
      "PurgeGuard re-checks the same condition in code before the first deletion.",
    blocking = true,
    locked = true)

  val ActiveRun = PurgeRule(
    id = "PC04",
    key = "active_run",
    title = "A job is writing here",
    detail = "run_history shows a job still RUNNING against this table, or one that finished inside " +
      "the configured quiet period. Deleting under a live writer corrupts the output of a job that " +
      "has not failed and will report success.",
    blocking = true)

  val DownstreamDependency = PurgeRule(
    id = "PC05",
    key = "downstream_dependency",
    title = "A downstream object depends on it",
    detail = "The dependency referential lists this table as feeding another object. The deletion " +
      "would not fail — it would break a consumer at its next run, somewhere else, later.",
    blocking = true)

  val MinVersionsKept = PurgeRule(
    id = "PC06",
    key = "min_versions_kept",
    title = "Minimum versions kept",
    detail = "The object is among the newest `keepMinVersions` vintages of its table, which the " +
      "policy requires to survive whatever their age. The rank is counted over EVERY vintage, " +
      "including those still inside retention, because that is the population the rule is about.",
    blocking = true)

  val NoArchive = PurgeRule(
    id = "PC07",
    key = "no_archive",
    title = "No archive copy",
    detail = "The policy demands a HARD, unrecoverable delete and an archive copy beforehand, and no " +
      "copy was found under the declared archive root. Not evaluated for a TRASH deletion, which is " +
      "recoverable on its own.",
    blocking = true)

  val NotAuthorized = PurgeRule(
    id = "PC08",
    key = "not_authorized",
    title = "Requester not authorised",
    detail = "The requester is not a member of the group that owns this perimeter. Checked against " +
      "the groups TWIST supplies for the requester; with no group list supplied the control reports " +
      "UNKNOWN rather than passing.",
    blocking = true)

  val BlastRadius = PurgeRule(
    id = "PC09",
    key = "blast_radius",
    title = "Blast radius",
    detail = "The run would exceed a ceiling: total bytes, total objects, or the share of a single " +
      "table it removes. A purge that is correct object by object can still be a mistake taken as a " +
      "whole. Cannot be switched off: PurgeGuard re-checks the ceilings in code.",
    blocking = true,
    locked = true)

  val RecentlyAccessed = PurgeRule(
    id = "PC10",
    key = "recently_accessed",
    title = "Recently accessed",
    detail = "The HDFS access time is inside the configured window, so someone may still be reading " +
      "it. A warning and not a veto: atime is unreliable — many clusters do not maintain it at all — " +
      "and a control that blocks on unreliable data teaches people to ignore controls.",
    blocking = false)

  val HiveMetadataOrphan = PurgeRule(
    id = "PC11",
    key = "hive_metadata_orphan",
    title = "Registered Hive partition",
    detail = "The path is a partition registered in the metastore, so the executor drops the " +
      "partition as well as removing the data. Every STR engine writes EXTERNAL tables with " +
      "external.table.purge = TRUE, which means DROP PARTITION deletes the data itself — the " +
      "metadata drop IS the deletion, and the executor must not also delete the path.",
    blocking = false)

  val ManifestDrift = PurgeRule(
    id = "PC12",
    key = "manifest_drift",
    title = "Manifest drift",
    detail = "At execution, the object's size or modification time no longer matches the manifest, " +
      "or the approval token does not match the manifest fingerprint. Evaluated only in the " +
      "execution phase — during a simulation there is nothing yet to have drifted from. Cannot be " +
      "switched off.",
    blocking = true,
    locked = true)

  val AlreadyAbsent = PurgeRule(
    id = "PC13",
    key = "already_absent",
    title = "Already absent",
    detail = "The path no longer exists. Evaluated in the execution phase, where it makes a replay " +
      "safe by recording SKIPPED_ABSENT instead of a failure; during a simulation the inventory has " +
      "just listed the object, so the question does not arise.",
    blocking = false)

  /**
   * PC14 — the object is something a run READ, not something it produced.
   *
   * An engine's model files, scenarios, rating scales and idealized matrices are shared by dozens of
   * runs and belong to none of them; so is the engine's own run history, which is the evidence that
   * a purge was legitimate. Purging "a run" must never reach them. The scope is drawn from the run's
   * declared OUTPUTS, so this should never fire — which is exactly why it is worth having: it fires
   * only when a policy pattern or a hand-picked path has been drawn wider than someone realised.
   */
  val SharedInput = PurgeRule(
    id = "PC14",
    key = "shared_input",
    title = "Shared input, not this run's output",
    detail = "The object is declared as an INPUT by one of the runs being purged, or is the engine's " +
      "own run history — or it is a directory that contains one. Inputs are shared across runs and " +
      "belong to none of them, and the run history is what proves the purge was authorised. Blocked " +
      "however the scope was drawn.",
    blocking = true)

  /**
   * PC15 — no retention policy governs this object.
   *
   * It is a warning and not a veto because the business owns its data and decides what to purge
   * (open question Q3). But the fact must not disappear: before that decision, PC01 blocked an
   * unmatched object and the report said so. This control keeps exactly that information visible —
   * "nothing but your own selection is protecting this object" — without standing in the way.
   */
  val NoRetentionPolicy = PurgeRule(
    id = "PC15",
    key = "no_retention_policy",
    title = "No retention policy",
    detail = "No policy in the retention referential matches this object, so nothing but the " +
      "explicit selection governs whether it may be deleted. Reported, not blocked: the business " +
      "owns its data. It becomes blocking again under " +
      "`retention_not_reached.blockWithoutPolicy = true`, which is the stricter default-deny reading.",
    blocking = false)

  /** Every control, in report order. */
  val All: Seq[PurgeRule] = Seq(
    RetentionNotReached, LegalHold, PathOutOfScope, ActiveRun, DownstreamDependency,
    MinVersionsKept, NoArchive, NotAuthorized, BlastRadius, SharedInput,
    NoRetentionPolicy, RecentlyAccessed, HiveMetadataOrphan, ManifestDrift, AlreadyAbsent)

  val byId: Map[String, PurgeRule] = All.map(rule => rule.id -> rule).toMap
}

/** One object a control fired on. `value` is whatever the control needs to justify itself. */
final case class PurgeFinding(path: String,
                              table: String,
                              value: String,
                              sizeBytes: Long)

/** Why a control did not produce a verdict. */
object NotEvaluated {
  /** Switched off in `purge_app.controls`. */
  val DISABLED = "DISABLED"
  /** Belongs to the execution phase; a simulation has nothing to evaluate it against. */
  val EXECUTE_PHASE = "EXECUTE_PHASE"
  /** The data the control needs is absent — an empty run_history, no dependency referential. */
  val NO_DATA = "NO_DATA"
}

/**
 * Outcome of one control.
 *
 * @param enabled      false when switched off in the conf — reported as SKIPPED, never as PASS
 * @param notEvaluated set when the control could not produce a verdict; carries which case it was
 * @param total        objects the control fired on
 * @param findings     the ones actually listed in the report (capped by `maxFindingsPerRule`)
 * @param bytes        bytes held by the objects it fired on
 */
final case class CheckRuleResult(rule: PurgeRule,
                                 enabled: Boolean,
                                 notEvaluated: Option[String],
                                 total: Long,
                                 findings: Seq[PurgeFinding],
                                 bytes: Long) {

  /** True when findings were listed but the report shows only a prefix of them. */
  def truncated: Boolean = total > findings.size

  /**
   * SKIPPED / NOT EVALUATED / UNKNOWN / PASS / BLOCKED(n) / WARNED(n).
   *
   * A control that could not run is never PASS. The distinction is the whole value of the report:
   * "we checked and found nothing" and "we could not check" lead to very different decisions, and
   * collapsing them into one green line is how a report stops being read.
   */
  def status: String =
    if (!enabled) "SKIPPED"
    else notEvaluated match {
      case Some(NotEvaluated.NO_DATA) => "UNKNOWN"
      case Some(_) => "NOT EVALUATED"
      case None if total == 0L => "PASS"
      case None if rule.blocking => s"BLOCKED($total)"
      case None => s"WARNED($total)"
    }

  /** What happened to the objects it fired on, for the report's Action column. */
  def action: String =
    if (!enabled) "-"
    else notEvaluated match {
      case Some(NotEvaluated.EXECUTE_PHASE) => "evaluated when the purge is executed"
      case Some(NotEvaluated.NO_DATA) => "the data this control needs was not available to this run"
      case Some(_) => "-"
      case None if total == 0L => "-"
      case None if rule.blocking => s"$total object(s) removed from what may be deleted"
      case None => s"$total object(s) flagged, still deletable"
    }
}

/**
 * The consolidated report for one simulation.
 *
 * @param source     what was inspected — the scan roots of the run
 * @param runId      the run this report belongs to, so it can be tied back to its execution
 * @param requestId  the TWIST request, empty for a run launched by hand
 * @param candidates objects the selection retained, before the controls
 * @param toDelete   objects no control blocked and none flagged
 * @param toWarn     objects a warning control flagged, still deletable
 * @param blocked    objects at least one blocking control removed
 * @param bytesTotal bytes held by every candidate
 * @param bytesToFree bytes held by the objects that may actually be deleted
 */
final case class CheckReport(source: String,
                             runId: String,
                             requestId: String,
                             generatedAt: String,
                             asOfDate: String,
                             manifestFingerprint: String,
                             candidates: Long,
                             toDelete: Long,
                             toWarn: Long,
                             blocked: Long,
                             bytesTotal: Long,
                             bytesToFree: Long,
                             results: Seq[CheckRuleResult]) {

  /** Overall verdict shown in the report header. */
  def verdict: String =
    if (candidates == 0L) "NOTHING SELECTED"
    else if (blocked > 0L) "BLOCKED"
    else if (toWarn > 0L) "WARNINGS"
    else "CLEAR"

  /** Controls that could not produce a verdict — the report's most important line after the verdict. */
  def unknowns: Seq[CheckRuleResult] =
    results.filter(r => r.enabled && r.notEvaluated.contains(NotEvaluated.NO_DATA))

  def blockingResults: Seq[CheckRuleResult] = results.filter(r => r.rule.blocking && r.total > 0L)

  def warningResults: Seq[CheckRuleResult] = results.filter(r => !r.rule.blocking && r.total > 0L)

  /** One-line summary for the run log. */
  def summaryLine: String =
    s"CONTROLS - $verdict: $candidates candidate(s) -> $toDelete deletable " +
      s"($toWarn with warning), $blocked blocked; " +
      s"${PrimaryUtilities.humanBytes(bytesToFree)} of ${PrimaryUtilities.humanBytes(bytesTotal)} would be freed" +
      (if (unknowns.isEmpty) "" else s"; ${unknowns.size} control(s) could not be checked: " +
        unknowns.map(_.rule.id).mkString(", ")) +
      " (nothing deleted: this run only simulates)"
}
