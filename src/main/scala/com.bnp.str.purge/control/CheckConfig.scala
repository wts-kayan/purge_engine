package com.bnp.str.purge.control

import com.bnp.str.purge.common.PrimaryRunner
import com.bnp.str.purge.utility.{PrimaryConstants, PrimaryUtilities}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

import java.time.LocalDate

/**
 * The `purge_app.controls { }` block, plus the few settings the controls need from elsewhere in the
 * conf (the allowed roots, the guard ceilings, who is asking).
 *
 * Reading them once, here, keeps two properties that matter:
 *  - a control never reaches into the raw config itself, so what each one depends on is visible in
 *    one file rather than scattered across thirteen rules;
 *  - `locked` rules cannot be switched off no matter what the conf says, and that is enforced at the
 *    single point where "is this rule enabled" is answered.
 */
final case class CheckConfig(enabled: Boolean,
                             maxFindingsPerRule: Int,
                             htmlPath: String,
                             controls: Config,
                             allowedRoots: Seq[String],
                             maxBytes: Long,
                             maxObjects: Long,
                             maxPercentOfTable: Int,
                             requester: String,
                             requesterGroups: Seq[String],
                             asOfDate: LocalDate) {

  /**
   * A control runs unless the conf switched it off — and a `locked` control runs regardless, because
   * PurgeGuard re-checks the same condition in code and a report that omitted it would describe a
   * run whose real behaviour is stricter than what it says.
   */
  def ruleEnabled(rule: PurgeRule): Boolean =
    rule.locked || PrimaryUtilities.getBooleanOr(ruleConfig(rule), "enabled", default = true)

  /** Rule-specific parameter, e.g. `active_run.minQuietMinutes`. */
  def ruleInt(rule: PurgeRule, key: String, default: Int): Int =
    PrimaryUtilities.getIntOr(ruleConfig(rule), key, default)

  def ruleString(rule: PurgeRule, key: String, default: String): String =
    PrimaryUtilities.getStringOr(ruleConfig(rule), key, default)

  def ruleBoolean(rule: PurgeRule, key: String, default: Boolean): Boolean =
    PrimaryUtilities.getBooleanOr(ruleConfig(rule), key, default)

  private def ruleConfig(rule: PurgeRule): Config =
    if (controls.hasPath(rule.key)) controls.getConfig(rule.key) else ConfigFactory.empty()
}

object CheckConfig {

  val CONTROLS = "controls"
  val GUARD = "guard"

  /** Ceilings default to "no ceiling"; the conf is where a perimeter sets its own. */
  val DEFAULT_MAX_BYTES = Long.MaxValue
  val DEFAULT_MAX_OBJECTS = Long.MaxValue
  val DEFAULT_MAX_PERCENT = 100

  def from(conf: Config)(implicit sparkSession: SparkSession): CheckConfig = {
    val appConf = PrimaryConstants.APP_CONF

    val controls =
      if (conf.hasPath(s"$appConf.$CONTROLS")) conf.getConfig(s"$appConf.$CONTROLS")
      else ConfigFactory.empty()

    val guard =
      if (conf.hasPath(s"$appConf.$GUARD")) conf.getConfig(s"$appConf.$GUARD") else ConfigFactory.empty()

    val request =
      if (conf.hasPath(s"$appConf.${PrimaryConstants.REQUEST}"))
        conf.getConfig(s"$appConf.${PrimaryConstants.REQUEST}")
      else ConfigFactory.empty()

    val scan = conf.getConfig(s"$appConf.${PrimaryConstants.SCAN}")

    CheckConfig(
      enabled = PrimaryUtilities.getBooleanOr(controls, "enabled", default = true),
      maxFindingsPerRule = PrimaryUtilities.getIntOr(controls, "maxFindingsPerRule", 200),
      htmlPath = PrimaryUtilities.getStringOr(controls, "htmlPath", ""),
      controls = controls,
      // the same resolution the inventory applies to its roots, so a containment test compares
      // like with like — see PrimaryUtilities.qualifyPath
      allowedRoots = PrimaryUtilities.getStringList(scan, PrimaryConstants.SCAN_ALLOWED_ROOTS)
        .map(PrimaryUtilities.qualifyPath),
      maxBytes = getLongOr(guard, "maxBytes", DEFAULT_MAX_BYTES),
      maxObjects = getLongOr(guard, "maxObjects", DEFAULT_MAX_OBJECTS),
      maxPercentOfTable = PrimaryUtilities.getIntOr(guard, "maxPercentOfTable", DEFAULT_MAX_PERCENT),
      requester = PrimaryUtilities.getStringOr(request, "requester", ""),
      // TWIST knows the requester's groups; the engine has no directory of its own, and guessing
      // membership would be worse than reporting PC08 as UNKNOWN
      requesterGroups = PrimaryUtilities.getStringList(request, "requesterGroups"),
      asOfDate = PrimaryRunner.asOfDate(conf))
  }

  private def getLongOr(cfg: Config, key: String, default: Long): Long =
    if (cfg.hasPath(key)) cfg.getLong(key) else default
}
