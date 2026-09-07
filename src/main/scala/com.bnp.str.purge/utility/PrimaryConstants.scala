package com.bnp.str.purge.utility

object PrimaryConstants {

  val APPLICATION_NAME = "purge_engine"
  val APP_CONF = "purge_app"

  // ---- input identifiers (config keys + PrimaryReader switch) ----
  val HDFS_INVENTORY = "hdfs_inventory"
  val HIVE_CATALOG = "hive_catalog"
  val PURGE_POLICY = "purge_policy"
  val PURGE_SCOPE = "purge_scope"
  val RUN_HISTORY = "run_history"

  // ---- temp-view names shared by the mapper (createOrReplaceTempView) and the selection SQL ----
  val VIEW_HDFS_INVENTORY = "hdfs_inventory_view"
  val VIEW_HIVE_CATALOG = "hive_catalog_view"
  val VIEW_PURGE_POLICY = "purge_policy_view"
  val VIEW_PURGE_SCOPE = "purge_scope_view"
  val VIEW_RUN_HISTORY = "run_history_view"

  // ---- outputs ----
  val PURGE_RUN_CATALOG = "purge_run_catalog" // P5: the runs on disk, for the IHM's scope screen
  val PURGE_INVENTORY = "purge_inventory" // P1: the raw scan of a scope
  val PURGE_CATALOG = "purge_catalog"     // P1: the metastore view of the same scope
  val PURGE_MANIFEST = "purge_manifest"   // P2: candidates + control decisions
  val PURGE_DETAIL = "purge_detail"       // P4: what the execution actually did

  // ---- config blocks ----
  val SCAN = "scan"
  val SCAN_ALLOWED_ROOTS = "allowedRoots"
  val SCAN_ROOTS = "roots"
  val SCAN_DATABASES = "databases"
  val SCAN_TABLES = "tables"
  val SCAN_MAX_DEPTH = "maxDepth"
  val SCAN_PARALLELISM = "parallelism"
  val SCAN_FOLLOW_SYMLINKS = "followSymlinks"

  val DEFAULT_MAX_DEPTH = 6
  val DEFAULT_PARALLELISM = 32

  // ---- object types emitted by the inventory ----
  /** A directory holding files and no sub-directory — the usual Hive partition folder. */
  val OBJECT_TYPE_LEAF_DIR = "LEAF_DIR"
  /** A file sitting directly under a directory that also has sub-directories. */
  val OBJECT_TYPE_FILE = "FILE"
  /** A directory the walk stopped at because `scan.maxDepth` was reached; stats come from the NameNode summary. */
  val OBJECT_TYPE_DIR_TRUNCATED = "DIR_TRUNCATED"

  // ---- catalog entry kinds ----
  val CATALOG_TABLE = "TABLE"
  val CATALOG_PARTITION = "PARTITION"

  // ---- decisions carried by the manifest ----
  /**
   * P2 writes this: selected by the retention policy or by the explicit scope, NOT yet judged by
   * the controls. It is the safe default — the P4 executor acts only on DELETE and
   * DELETE_WITH_WARNING, so a manifest that never reached the controls cannot delete anything.
   */
  val DECISION_CANDIDATE = "CANDIDATE"
  val DECISION_DELETE = "DELETE"
  val DECISION_WARN = "DELETE_WITH_WARNING"
  val DECISION_BLOCKED = "BLOCKED"

  /** The only decisions the executor may act on. Everything else is left alone. */
  val DELETABLE_DECISIONS = Seq(DECISION_DELETE, DECISION_WARN)

  // ---- why a candidate is in the manifest ----
  val SELECTION_POLICY = "POLICY" // its age passed the retention of the policy that matched it
  val SELECTION_SCOPE = "SCOPE"   // a person picked it in TWIST, or an engine run expanded to it
  val SELECTION_BOTH = "POLICY+SCOPE"

  // ---- where a candidate's age was read from ----
  val DATE_SOURCE_RUN = "RUN"             // the as-of date of the engine run that produced it
  val DATE_SOURCE_PARTITION = "PARTITION" // the business date carried by the partition value
  val DATE_SOURCE_MTIME = "MTIME"         // the HDFS modification time, when there is nothing better

  // ---- request / policy config ----
  val ENGINE = "engine"
  val REQUEST = "request"
  val GUARD = "guard"
  val EXECUTION = "execution"
  val PURGE_SCOPE_ENABLE = "enable"
  val POLICY_SOURCE_CONF = "conf"
  val POLICY_SOURCE_TABLE = "table"

  // ---- deletion strategies a policy may name (executed at P4) ----
  val STRATEGY_TRASH = "TRASH"
  val STRATEGY_HARD = "HARD"
  val STRATEGY_DROP_PARTITION = "DROP_PARTITION"
  val STRATEGY_LOGICAL = "LOGICAL"
  val STRATEGIES = Seq(STRATEGY_TRASH, STRATEGY_HARD, STRATEGY_DROP_PARTITION, STRATEGY_LOGICAL)

  // ---- retention units accepted by a purge policy ----
  val UNIT_DAY = "DAY"
  val UNIT_MONTH = "MONTH"
  val UNIT_QUARTER = "QUARTER"
  val UNIT_YEAR = "YEAR"
  val RETENTION_UNITS = Seq(UNIT_DAY, UNIT_MONTH, UNIT_QUARTER, UNIT_YEAR)

  // ---------------------------------------------------------------------------------------------
  // GUARD — deliberately NOT configurable.
  //
  // `controls { }` in the application.conf can switch rules off; these cannot be switched off,
  // because the configuration is exactly what may be wrong. Every value below is duplicated by a
  // control rule on purpose (see PC03 / PC09 in the technical specification).
  // ---------------------------------------------------------------------------------------------

  /** A purge target must have at least this many path segments. `/data/promethee/str/rwa` = 4. */
  val MIN_PATH_DEPTH = 4

  /** Paths that may never be a purge target nor a scan root, whatever the configuration says. */
  val FORBIDDEN_PATHS: Set[String] =
    Set("/", "/user", "/tmp", "/apps", "/warehouse", "/hive", "/hbase", "/system", "/var")

  val MODE_OVERWRITE = "Overwrite"
  val MODE_APPEND = "Append"
}
