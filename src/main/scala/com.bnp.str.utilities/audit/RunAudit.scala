package com.bnp.str.utilities.audit

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.sql.Timestamp
import java.util.UUID

/**
 * Reusable, MODULE-AGNOSTIC run auditor for the whole jar. Any module (tseadfwd, climatetables,
 * addons, excelor, …) records its execution to the shared `run_history` ORC external table with
 * one call at start and one at the end:
 *
 * {{{
 *   val audit = RunAudit.start("tseadfwd", auditConfig, usedConf = confPath)(spark)
 *   try {
 *     run()
 *     audit.succeeded()
 *   } catch { case e: Throwable => audit.failed(e); throw e }
 * }}}
 *
 * Storage is an EXTERNAL Hive table stored as ORC, PARTITIONED BY (module_name, run_id) — see
 * [[RunAuditStore]]. A row is written at start (end_date/duration null) and replaced in place with
 * the finalized row at the end. All audit IO is guarded: a failure only logs a warning and never
 * breaks the job.
 *
 * Identity/metadata that a launcher supplies is resolved per field:
 * explicit override > `-Drun.*` system property > `RUN_*` env var > config > default.
 */
class RunAudit private(private var record: RunAuditRecord,
                       table: String,
                       location: String,
                       enabled: Boolean,
                       startNanos: Long)
                      (implicit spark: SparkSession) {

  import RunAudit.log

  /** Unique id of this run (echo it back to the launcher / logs to correlate). */
  def runId: String = record.runId

  /** The current (possibly not-yet-finished) record. */
  def current: RunAuditRecord = record

  /** Mark the run SUCCESS and persist the final record. */
  def succeeded(): RunAuditRecord = finish(RunStatus.SUCCESS, None)

  /** Mark the run FAILED (logs the throwable) and persist the final record. */
  def failed(t: Throwable): RunAuditRecord = finish(RunStatus.FAILED, Option(t))

  private def finish(status: String, error: Option[Throwable]): RunAuditRecord = {
    val endTs      = new Timestamp(System.currentTimeMillis())
    val durationMs = (System.nanoTime() - startNanos) / 1000000L
    error.foreach(e => log.error(s"[audit] run ${record.runId} (${record.moduleName}) failed", e))
    record = record.copy(
      status   = status,
      endDate  = Some(endTs),
      duration = Some(RunAudit.formatDuration(durationMs))
    )
    persist()
    record
  }

  /** Write the current record to its (module_name, run_id) ORC partition. Guarded: never propagates. */
  private def persist(): Unit = {
    if (!enabled) return
    try {
      RunAuditStore.write(record, table, location)
      log.info(s"[audit] run ${record.runId} (${record.moduleName}) status=${record.status} -> $table")
    } catch {
      case e: Throwable =>
        log.warn(s"[audit] failed to write run_history for run ${record.runId} " +
          s"(${record.moduleName}); continuing without audit: " +
          s"${e.getClass.getSimpleName}: ${e.getMessage}")
    }
  }
}

object RunAudit {

  private val log = LoggerFactory.getLogger(classOf[RunAudit])

  /** Format an elapsed millisecond count as e.g. "0h 20mn 27s". */
  def formatDuration(ms: Long): String = {
    val totalSec = ms / 1000
    val h = totalSec / 3600
    val m = (totalSec % 3600) / 60
    val s = totalSec % 60
    s"${h}h ${m}mn ${s}s"
  }

  /**
   * Begin auditing a run: build the record, resolve launcher-supplied metadata, and write the
   * initial row (end_date/duration null) so an interrupted run is still visible. Returns a handle
   * to finish with [[succeeded]] / [[failed]].
   *
   * @param moduleName       module that owns the run (PARTITION), e.g. "tseadfwd", "climatetables"
   * @param auditConfig      the `audit { }` config block (keys: enabled, table, database, root/location,
   *                         runId, userLauncher, motor, usedJar — all optional). When `database` is set
   *                         the table LOCATION is derived from that Hive database's own warehouse dir
   *                         (`<db.locationUri>/<table>`); otherwise `root`/`location` is used.
   * @param usedConf         path of the application.conf used
   * @param runId            override for the run id (else config `runId`, else a generated UUID)
   * @param userLauncher     override for the launching user (else -D/env/config, else JVM user.name)
   * @param motor            override for the compute motor (else config `motor`, else "UNKNOWN")
   * @param usedJar          override for the launched jar name (else config `usedJar`, else auto-detected)
   * @param projectionDates  module-specific projection years string, e.g. "[2030, 2040, 2050]"
   * @param scenarios        module-specific scenarios string, e.g. "[\"FW\", \"NZ50\"]"
   * @param baseFolderName   module-specific base folder name
   * @param spark            the SparkSession (Hive-enabled on the cluster) used to write the ORC table
   */
  def start(moduleName: String,
            auditConfig: Config,
            usedConf: String = "",
            runId: Option[String] = None,
            userLauncher: Option[String] = None,
            motor: Option[String] = None,
            usedJar: Option[String] = None,
            projectionDates: Option[String] = None,
            scenarios: Option[String] = None,
            baseFolderName: Option[String] = None)
           (implicit spark: SparkSession): RunAudit = {

    val enabled  = getBool(auditConfig, "enabled", default = true)
    val database = getOpt(auditConfig, "database")
    val rawTable = getStr(auditConfig, "table", default = RunAuditStore.DEFAULT_TABLE)
    val (table, location) = resolveTableAndLocation(auditConfig, database, rawTable)

    val resolvedRunId =
      runId.filter(nonBlank)
        .orElse(if (auditConfig.hasPath("runId")) Some(auditConfig.getString("runId")).filter(nonBlank) else None)
        .getOrElse(UUID.randomUUID().toString)

    val record = RunAuditRecord(
      runId         = resolvedRunId,
      applicationId = try spark.sparkContext.applicationId catch { case _: Throwable => "UNKNOWN" },
      moduleName    = moduleName,
      usedJar       = usedJar.filter(nonBlank)
                        .orElse(Option(System.getProperty("run.usedJar")).filter(nonBlank))
                        .orElse(Option(System.getenv("RUN_USED_JAR")).filter(nonBlank))
                        .orElse(getOpt(auditConfig, "usedJar"))
                        .orElse(detectJar)
                        .getOrElse("UNKNOWN"),
      usedConf      = usedConf,
      userLauncher  = resolve(userLauncher, "run.userLauncher", "RUN_USER_LAUNCHER",
                              auditConfig, "userLauncher", System.getProperty("user.name", "UNKNOWN")),
      status        = RunStatus.RUNNING,
      creationDate  = new Timestamp(System.currentTimeMillis()),
      endDate       = None,
      duration      = None,
      motor         = resolve(motor, "run.motor", "RUN_MOTOR", auditConfig, "motor", "UNKNOWN"),
      projectionDates = projectionDates.filter(nonBlank),
      scenarios       = scenarios.filter(nonBlank),
      baseFolderName  = baseFolderName.filter(nonBlank)
    )

    val audit = new RunAudit(record, table, location, enabled, System.nanoTime())
    audit.persist()
    audit
  }

  // ---- config / identity resolution helpers ----

  /**
   * Resolve the (qualified table name, storage LOCATION) pair used to write the audit ORC.
   *
   * When a Hive `database` is configured, the table lives inside that database and its LOCATION is
   * derived dynamically from the database's own warehouse directory — `<db.locationUri>/<table>` —
   * so nothing is hard-coded and the ORC lands where Hive expects the database's tables:
   * {{{
   *   val dbPath   = spark.catalog.getDatabase(database).locationUri
   *   val location = dbPath + "/" + table.toLowerCase
   * }}}
   * Falls back to the config `root`/`location` (normalized to an absolute local path) with an
   * unqualified table name when no database is set, or when the metastore lookup fails (e.g. a local
   * box without Hive).
   */
  private def resolveTableAndLocation(cfg: Config, database: Option[String], table: String)
                                     (implicit spark: SparkSession): (String, String) =
    database.filter(nonBlank) match {
      case Some(db) =>
        try {
          val dbPath   = spark.catalog.getDatabase(db).locationUri
          val location = s"${dbPath.stripSuffix("/")}/${table.toLowerCase}"
          (s"$db.$table", location)
        } catch {
          case e: Throwable =>
            log.warn(s"[audit] could not resolve location from database '$db' " +
              s"(${e.getClass.getSimpleName}: ${e.getMessage}); falling back to config root")
            (table, normalizeLocation(resolveRoot(cfg)))
        }
      case None =>
        (table, normalizeLocation(resolveRoot(cfg)))
    }

  /** Audit table LOCATION: `root` (preferred) or legacy `location` key. */
  private def resolveRoot(cfg: Config): String =
    if (cfg.hasPath("root")) cfg.getString("root")
    else if (cfg.hasPath("location")) cfg.getString("location")
    else "run_history"

  /** Make a scheme-less relative path absolute so Hive's LOCATION resolves against the local FS. */
  private def normalizeLocation(root: String): String =
    if (root.contains("://")) root
    else new java.io.File(root).getAbsolutePath.replace('\\', '/')

  /** override > -D<sysProp> > env(<envVar>) > config(<cfgKey>) > default. Blanks are ignored. */
  private def resolve(overrideVal: Option[String],
                      sysProp: String,
                      envVar: String,
                      cfg: Config,
                      cfgKey: String,
                      default: String): String =
    overrideVal.filter(nonBlank)
      .orElse(Option(System.getProperty(sysProp)).filter(nonBlank))
      .orElse(Option(System.getenv(envVar)).filter(nonBlank))
      .orElse(getOpt(cfg, cfgKey))
      .getOrElse(default)

  private def getOpt(cfg: Config, key: String): Option[String] =
    if (cfg.hasPath(key)) Some(cfg.getString(key)).filter(nonBlank) else None

  private def nonBlank(s: String): Boolean = s != null && s.trim.nonEmpty

  private def getStr(cfg: Config, key: String, default: String): String =
    if (cfg.hasPath(key)) cfg.getString(key) else default

  private def getBool(cfg: Config, key: String, default: Boolean): Boolean =
    if (cfg.hasPath(key)) cfg.getBoolean(key) else default

  /**
   * On YARN the submitted application jar is localized on the cluster under the fixed placeholder
   * name `__app__.jar`, so the classloader code source reports that placeholder instead of the real
   * file name. Treat it as "not a real name" so detection falls through to the Spark config.
   */
  private val YarnAppJarPlaceholder = "__app__.jar"

  /**
   * Best-effort file name of the jar this code was launched from. Preference order:
   *   1. the classloader code-source basename (works for an IDE / plain `java -jar` / YARN client run);
   *   2. the real submitted jar recovered from the Spark configuration (YARN cluster mode, where the
   *      code source is the useless `__app__.jar` placeholder).
   * Returns None only when neither yields a usable `*.jar` name, in which case the caller falls back
   * to "UNKNOWN". A launcher can always bypass detection via `-Drun.usedJar` / `RUN_USED_JAR` / config.
   */
  private def detectJar(implicit spark: SparkSession): Option[String] =
    codeSourceJar.orElse(jarFromSparkConf)

  /** Basename of the jar this code was loaded from, ignoring the YARN `__app__.jar` placeholder. */
  private def codeSourceJar: Option[String] =
    try {
      val path = classOf[RunAudit].getProtectionDomain.getCodeSource.getLocation.toURI.getPath
      Some(jarBaseName(path)).filter(nonBlank).filterNot(_ == YarnAppJarPlaceholder)
    } catch { case _: Throwable => None }

  /**
   * Recover the real submitted jar name from the Spark configuration, for the YARN-cluster case where
   * the code source is `__app__.jar`. Scans `spark.jars` then `spark.yarn.dist.jars` for the first
   * `*.jar` basename that is not the placeholder. Best-effort: returns None if nothing usable is found.
   */
  private def jarFromSparkConf(implicit spark: SparkSession): Option[String] =
    try {
      val conf = spark.sparkContext.getConf
      Seq("spark.jars", "spark.yarn.dist.jars")
        .flatMap(k => conf.getOption(k).toSeq)
        .flatMap(_.split(","))
        .map(_.trim).filter(nonBlank)
        .map(jarBaseName)
        .filter(_.toLowerCase.endsWith(".jar"))
        .filterNot(_ == YarnAppJarPlaceholder)
        .headOption
    } catch { case _: Throwable => None }

  /** Basename of a path: everything after the last '/' or '\' (the whole path when neither is present). */
  private def jarBaseName(path: String): String = {
    if (path == null || path.isEmpty) return ""
    var idx = path.lastIndexOf('/')
    if (idx == -1) idx = path.lastIndexOf('\\')
    path.substring(idx + 1)
  }
}
