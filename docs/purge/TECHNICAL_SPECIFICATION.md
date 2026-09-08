# Purge Engine — Technical Specification (Design)

> Status: **v0.4 — P1 to P4 implemented plus the P5 engine side, projection engine as MVP** (skeleton, inventory,
> catalog, engine-aware run scoping, retention policies, candidate selection, fingerprinted
> manifest, the fifteen controls, the HTML report — and, since P4, the execution: PurgeGuard,
> PurgeExecutor, purge_detail and the run audit. **The engine now deletes.** P5 adds the run
> catalogue the IHM's scope screen needs; the TWIST screens themselves are the IHM team's work.
> P6 (restore) is design. **The twelve open questions were answered by the
> business on 2026-08-29** — see `docs/purge/OPEN_QUESTIONS_PURGE.xlsx`; §16 records what
> those answers changed.
> Cluster: **Promethee** (Cloudera), Datalake BNP.
> Launcher: **TWIST** (in-house IHM), same contract as the other Spark engines.
> Reference implementation followed: `file_transform_engine` — package
> `com.bnp.str.addons` (structure, file names, class names, conf conventions) plus the
> `com.bnp.str.tseadfwd.coherence` control pattern and the shared
> `com.bnp.str.utilities.audit` run auditing.

---

## 1. Context and objective

Today the Datalake on Promethee is **only written to**. Engines (`addons`, `ageing`,
`tseadfwd`, …) read, transform and write ORC (and CSV / Excel / Parquet) outputs, run after run,
quarter after quarter. Nothing removes the obsolete vintages: storage grows, HDFS small-file
pressure grows, and there is no traceable answer to "who deleted what, when, and was it allowed?".

The **Purge Engine** closes that loop. It is a new Spark engine in the same jar family, launched by
TWIST exactly like the existing ones, whose job is to:

1. **Inventory** what exists in a declared scope (HDFS paths and/or Hive tables & partitions);
2. **Select** the candidates for deletion from a retention policy or an explicit user selection;
3. **Control** every candidate against a set of business and technical rules — *can this really be
   deleted?* — and produce a human-readable report;
4. **Execute** the deletion of the approved candidates only, with a recoverable strategy;
5. **Audit** everything, at run level and at object level, in immutable ORC tables.

A dedicated **TWIST IHM** drives the whole thing: choose the data, run the analysis, read the
control report, approve, execute, and consult / restore afterwards.

### Design principle: never delete in one shot

The engine is **two-phase by construction**. A purge is *always*:

```
SIMULATE  ->  (manifest + control report)  ->  human APPROVAL  ->  EXECUTE (manifest only)
```

The execution phase never re-derives what to delete. It reads the manifest produced by the
simulation, re-verifies it, and deletes strictly what is listed and approved. Anything that drifted
between the two phases aborts the run.

---

## 2. Scope

### In scope (v1)

| | |
|---|---|
| Object types | HDFS directories/files, Hive external tables, Hive partitions |
| Formats | ORC first; format-agnostic at path level (CSV, Parquet, Excel, JSON also covered) |
| Selection | by retention policy, by explicit selection in TWIST, by `run_id`, by date/quarter range |
| Deletion | HDFS Trash (default, recoverable), hard delete, `DROP PARTITION`, logical delete |
| Controls | 15 rules, blocking or warning (§8) |
| Audit | run level (shared `run_history`) + object level (`purge_detail`) |
| IHM | scope selection, simulation, report, 4-eyes approval, execution, history, restore |

### Out of scope (v1)

- Purging inside a file (row-level deletion / GDPR "right to be forgotten" on individual records).
  v1 deletes at *object* granularity (path / partition). Row-level is a v2 topic (§15).
- Kudu / HBase / Solr / Impala-managed storage.
- Cross-cluster (DR) propagation of a deletion.
- Automatic scheduling. v1 is launched by TWIST on demand; a scheduled mode comes later.

---

## 3. Vocabulary

| Term | Meaning |
|---|---|
| **Scope** | What the user pointed at: a set of roots, tables, patterns, and a date filter. |
| **Candidate** | One concrete object (a path or a `table/partition`) that the selection retained. |
| **Policy** | A retention rule attached to a domain/table/path pattern: how long to keep, how to delete, minimum versions to keep. |
| **Control** | A rule answering "may this candidate be deleted?" — `BLOCKING` or `WARNING`. |
| **Manifest** | The frozen, fingerprinted list of candidates with their control verdicts, produced by a simulation run. |
| **Decision** | Per candidate: `DELETE`, `BLOCKED`, or `DELETE_WITH_WARNING`. |
| **Request** | The TWIST object binding a scope, a simulation, an approval and an execution. |

---

## 3bis. The unit of a purge: a RUN, not a date

The design above reasons about *vintages* — an `as_of_date` partition ages out and may go. That is
the right unit for a date-partitioned output and the **wrong one for most of the engines on
Promethee**, which partition by RUN.

A projection run writes `runId=<uuid>` into thirty-odd tables at once:

```
/Projects/STCreditRisk_STE/hive/databases/dbprojection.db/
+-- term_structure/          runId=9df8cf3a-.../  runId=5c1f53c8-.../  ...(~183 of them)
+-- term_structure_detailed/ runId=9df8cf3a-.../  ...
+-- projected_dr/            runId=9df8cf3a-.../  ...
+-- migration_matrix/  lgd/  pcure/  chr_detailed/  scenarii_ponderation/  ...
+-- run_history/             <- the engine's own audit, never a purge target
```

Those partitions are **one thing**: produced together by one execution, meaningful together, and
purged together or not at all. Purging "the 2023 partitions of `term_structure`" would leave the
same run's rows in twenty-nine sibling tables — a half-deleted run that no consumer can interpret
and no report can explain.

So **a purge request names an engine and one or more of its runs.**

### Where the truth about a run comes from

Not from a naming convention, and not from the metastore: from **the engine's own run
configuration**, the `conf.properties` it was launched with, which TWIST already keeps. That file
declares everything a purge needs:

| What | Key(s) in the projection `conf.properties` |
|---|---|
| the run's identity | `projection.run.id`, `projection.name`, `projection.run.type`, `projection.twist.user` |
| the run's business date | `parameter.asOfDateQuarter` |
| where it wrote | `output.database.name` + every `output.table.name.*` |
| its non-Hive output | `cluster.output.directory`, `output.hdfs.directory` |
| what it READ | every `input.path.*` |
| the engine's own history | `projection.history.table.name` |

Three readings of that file are not obvious, and each one is a way to get a purge wrong:

1. **Table names are lowercased.** The conf spells `projected_CR_detailed`; Hive puts
   `projected_cr_detailed` on disk. A path built from the conf spelling does not exist — and a purge
   that silently finds nothing looks exactly like a purge that had nothing to do.
2. **The partition key is spelled two ways.** On disk it is `runId=` (capital I, as observed under
   `dbprojection.db/term_structure`); the metastore lowercases it to `runid=`. The engine emits both
   spellings so neither side has to guess, and every partition spec is key-lowercased before it is
   compared.
3. **`input.path.*` are SHARED and `run_history` is evidence.** Models, scenarios, rating scales and
   idealized matrices are read by dozens of runs and belong to none of them; the run history is what
   proves the purge was authorised. Both are collected as **protected paths** and enforced by the
   new control **PC14**.

### How a run becomes a scope

Each run expands, for every output table it declares, into two scope entries:

- a metastore identity `(database, table, runid=<uuid>)`, and
- a path `<database location>/<table>/runId=<uuid>`.

**Both**, on purpose. The metastore and the filesystem disagree more often than anyone would like: a
partition dropped from the catalogue but still on disk is precisely the leftover a purge exists to
remove, and one registered but already gone must still be reported rather than silently missed.

Both shapes above are the **PARTITION-granular** ones. An engine whose run *is* the table — the
simulator, per §16.Q1 — expands instead to `(database, table, '')` and
`<database location>/<table>`, with no partition segment at all. Which of the two is built is
decided by the descriptor's `granularity` and nothing else (`EngineRun.relativePathOf` /
`partitionSpecOf`); no caller assumes a shape. **This is not cosmetic.** Build the partition shape
for a table-granular engine and the scope names directories that were never written: no error, no
candidates, a report that says zero, and a run that succeeded at removing nothing — the failure mode
this codebase keeps returning to. The same reasoning fixes the run catalogue, which recovers a run
from the `runId=` segment when the engine is partition-granular and from the table directory when it
is not, and control PC04, whose "is a job writing here?" token is `runId=<uuid>` in one case and the
table name in the other.

### An explicit selection is exclusive

When anything is explicitly selected — an engine run, or a hand-picked path — **that selection is
the whole row set**; the retention policy no longer *selects*, it only *governs*.

This closes a trap that is easy to miss and expensive to hit. A sibling run whose files merely look
old would otherwise be swept in beside the run that was actually chosen, because a run partition
carries no date and the file mtime is all a policy has to go on. The policy still applies to what
was selected — PC01 blocks a run whose retention is not reached, and default-deny still applies to
one matching no policy at all — it simply does not go hunting for more.

### The age of a run

For run-partitioned output the truthful age is the **run's own as-of quarter**, not the date its
files were written. A 2023Q1 projection re-run last week is still 2023Q1 data. The engine therefore
resolves the age in order of truthfulness, and records which one it used in `business_date_source`:

| Source | Meaning |
|---|---|
| `RUN` | the as-of date of the engine run that produced the object |
| `PARTITION` | the business date in the partition value (`as_of_date=2023-03-31`) |
| `MTIME` | the file modification time — the last resort, and actively misleading for a re-run |

---

## 4. Architecture overview

```
      ┌────────────────────────────────────────────────────────────────────┐
      │                          TWIST  (IHM)                              │
      │  Scope  ->  Simulate  ->  Report  ->  Approve (4-eyes)  ->  Execute │
      │                          History / Restore                         │
      └───────────────┬──────────────────────────────────┬─────────────────┘
                      │ writes application.conf          │ reads
                      │ spark-submit                     │
                      v                                  │
      ┌────────────────────────────────────────────────┐ │
      │   PURGE ENGINE  (Spark, com.bnp.str.purge)     │ │
      │                                                │ │
      │   MainDriver / SimulationDriver / RestoreDriver│ │
      │        │                                       │ │
      │   PrimaryReader ─ inventory (HDFS + metastore) │ │
      │        │                                       │ │
      │   PrimaryMapper ─ candidate selection (SQL)    │ │
      │        │                                       │ │
      │   PurgeControlMapper ─ 13 controls             │ │
      │        │                                       │ │
      │   PrimaryWriter ─ manifest ORC + HTML report   │ │
      │        │                                       │ │
      │   PurgeExecutor ─ Trash / Hard / DropPartition │ │
      │        │                                       │ │
      │   PurgeAudit ─ run_history + purge_detail  ────┼─┘
      └────────────────────────────────────────────────┘
                      │
                      v
      ┌────────────────────────────────────────────────┐
      │  PROMETHEE — HDFS  +  Hive metastore  +  Trash  │
      └────────────────────────────────────────────────┘
```

TWIST talks to the engine the way it already talks to `addons` / `ageing` / `tseadfwd`: it
**generates a HOCON `application.conf`** and runs
`spark-submit --class com.bnp.str.purge.job.MainDriver <jar> <conf-path>`. No new protocol, no
REST server inside the engine. Everything the engine needs — mode, request id, scope, approval
token, audit metadata — lives in that conf.

---

## 5. Purge lifecycle

| State | Set by | Meaning |
|---|---|---|
| `DRAFT` | TWIST | The user is building the scope. Nothing has run. |
| `SIMULATING` | Engine (`SimulationDriver`) | Inventory + selection + controls running. |
| `SIMULATED` | Engine | Manifest + HTML report available. Nothing deleted. |
| `REJECTED` | TWIST | Approver refused, or the requester dropped the request. Terminal. |
| `APPROVED` | TWIST | 4-eyes validation passed; an approval token is stamped on the request. |
| `EXECUTING` | Engine (`MainDriver`) | Deletion in progress. |
| `EXECUTED` | Engine | Deletion done. `purge_detail` holds one row per object. |
| `PARTIAL` | Engine | Some objects failed; the failures are listed and the run is not `SUCCESS`. |
| `RESTORED` | Engine (`RestoreDriver`) | Objects moved back from Trash within the retention window. |

`SIMULATED` expires: a manifest older than `guard.manifestMaxAgeHours` (default 72h) may not be
executed — the world moves. TWIST re-runs the simulation instead.

---

## 6. Project structure

Same layout, same file names and same class-name logic as
`file_transform_engine/src/main/scala/com.bnp.str.addons`, extended with the packages the purge
concern genuinely needs (`control` mirrors `tseadfwd/coherence`, `audit` mirrors
`tseadfwd/audit`, `purge` is the only engine-specific addition).

```
purge_engine/
├── pom.xml                                   # cloned from file_transform_engine (Scala 2.12.18 / Spark 3.5.4)
├── docs/purge/TECHNICAL_SPECIFICATION.md     # this document
├── localRun/
│   └── purge/
│       ├── application.conf                  # local run conf
│       ├── input/                            # fake datalake tree for local tests
│       └── output/                           # manifest + reports
└── src/
    ├── main/
    │   ├── resources/
    │   └── scala/
    │       └── com.bnp.str.purge/
    │           ├── job/
    │           │   ├── InventoryDriver.scala      # P1 entry point — inventory only, read-only
    │           │   ├── MainDriver.scala           # entry point — simulate and/or execute
    │           │   ├── SimulationDriver.scala     # entry point — simulate only (TWIST "Analyse")
    │           │   └── RestoreDriver.scala        # entry point — restore from Trash
    │           ├── common/
    │           │   ├── RunnerProvider.scala       # abstract, lazily exposes the reader inputs
    │           │   ├── PrimaryRunner.scala        # orchestration: inventory -> select -> control
    │           │   └── MapperProvider.scala       # abstract mapper contract
    │           ├── engine/                        # what an engine is, and what a run of it produced
    │           │   ├── EngineRun.scala            # one execution: its outputs, inputs, identity
    │           │   ├── EngineDescriptor.scala     # per-engine conventions + the registry
    │           │   └── ProjectionEngine.scala     # the projection conf.properties (MVP)
    │           ├── reader/
    │           │   ├── PrimaryReader.scala        # named inputs -> DataFrame (switch by name)
    │           │   ├── InventoryReader.scala      # HDFS walk -> file/dir inventory DataFrame
    │           │   ├── CatalogReader.scala        # Hive metastore -> tables/partitions DataFrame
    │           │   ├── EngineRunReader.scala      # engine run(s) -> scope + protected paths
    │           │   └── PolicyReader.scala         # retention policy referential (conf or table)
    │           ├── mapping/
    │           │   ├── PrimaryMapper.scala        # builds the candidate set
    │           │   ├── PrimaryView.scala          # the candidate-selection SQL
    │           │   └── ManifestView.scala         # manifest schema + fingerprint
    │           ├── control/
    │           │   ├── CheckModel.scala           # PurgeRule / PurgeFinding / CheckRuleResult / CheckReport
    │           │   ├── CheckConfig.scala          # the `controls { }` conf block
    │           │   ├── PurgeControlMapper.scala   # evaluates the 13 rules, returns decisions
    │           │   ├── CheckHtmlView.scala        # renders the HTML report
    │           │   └── CheckWriter.scala          # writes the report (local or HDFS)
    │           ├── purge/                         # the only destructive package
    │           │   ├── PurgeStrategy.scala        # Trash | Hard | DropPartition | Logical
    │           │   ├── PurgeExecutor.scala        # performs the deletion, object by object
    │           │   └── PurgeGuard.scala           # hard-coded safety net (whitelist, depth, volume)
    │           ├── writer/
    │           │   ├── PrimaryWriter.scala        # writes the manifest ORC
    │           │   └── PurgeDetailWriter.scala    # writes the per-object execution result
    │           ├── audit/
    │           │   ├── PurgeAudit.scala           # module wiring on the shared RunAudit
    │           │   └── PurgeDetailStore.scala     # `purge_detail` external ORC table
    │           ├── sessionmanager/
    │           │   └── StrSparkSessionManager.scala
    │           └── utility/
    │               ├── PrimaryConstants.scala
    │               ├── PrimaryUtilities.scala
    │               └── DateUtils.scala            # retention date math (age, quarter, as-of)
    └── test/
        └── scala/
            └── com.bnp.str.purge/
                ├── SparkTestSession.scala
                ├── PrimaryUtilitiesSpec.scala
                ├── PrimaryViewSpec.scala
                ├── PurgeControlMapperSpec.scala
                ├── EngineGranularitySpec.scala
                ├── PurgeGuardSpec.scala
                ├── PurgeExecutorSpec.scala
                └── CheckHtmlViewSpec.scala
```

The shared `com.bnp.str.utilities` package (`SparkConfLogger`, `audit/RunAudit`,
`audit/RunAuditRecord`, `audit/RunAuditStore`) is **reused as-is**, either by packaging the purge
engine in the same jar or by extracting that package into a small shared artifact. Recommended:
same jar, one more module — it is exactly what `addons` / `ageing` / `tseadfwd` already do, and
`run_history` is already module-agnostic (`module_name` = `purge`).

---

## 7. Class-by-class specification

### 7.1 `job/MainDriver.scala`

Same shape as `com.bnp.str.addons.job.MainDriver` and `com.bnp.str.tseadfwd.job.MainDriver`:
`main(args)` takes the conf path, builds the session from `StrSparkSessionManager`, parses the conf
through `PrimaryUtilities.getHdfsReader`, opens the run audit, runs, closes the audit, rethrows on
failure.

```scala
object MainDriver {
  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "Missing argument: path to the application.conf")
    val absoluteConfigPath = args(0)

    val sparkSession = StrSparkSessionManager.fetchSparkSession(PrimaryConstants.APPLICATION_NAME)
    val config       = ConfigFactory.parseReader(
                         PrimaryUtilities.getHdfsReader(absoluteConfigPath)(sparkSession.sparkContext))

    val audit = PurgeAudit.start(config, usedConf = absoluteConfigPath)(sparkSession)

    try {
      val primaryReader = new PrimaryReader()(sparkSession, config)
      val primaryWriter = new PrimaryWriter()(sparkSession, config)
      val mode          = PurgeMode.from(config)      // SIMULATE | EXECUTE | SIMULATE_AND_EXECUTE

      // ---- phase 1: inventory -> candidates -> controls -> manifest + HTML report ----
      val outcome = new PrimaryRunner(primaryReader, PrimaryConstants.PURGE_MANIFEST)(sparkSession, config)
                      .run_purge_runner(audit.runId)

      log.info(outcome.report.summaryLine)
      CheckWriter.writeHtml(checks.htmlPath, outcome.report)(sparkSession)
      primaryWriter.write(outcome.manifest, PrimaryConstants.PURGE_MANIFEST)(sparkSession, config)

      // ---- phase 2: deletion, manifest only, approved rows only ----
      if (mode.executes) {
        PurgeGuard.assertSafe(outcome.manifest)(sparkSession, config)   // hard safety net
        val result = new PurgeExecutor()(sparkSession, config).execute(outcome.manifest, audit.runId)
        PurgeDetailWriter.write(result, audit.runId)(sparkSession, config)
        log.info(result.summaryLine)
      } else {
        log.info("mode = SIMULATE -> nothing deleted; manifest and report written")
      }

      audit.succeeded()
    } catch { case e: Throwable => audit.failed(e); throw e }
  }
}
```

`SimulationDriver` is a thin `main` that forces `mode = SIMULATE` and delegates, so TWIST's
"Analyse" button can never physically delete, whatever the conf says.

### 7.2 `common/RunnerProvider.scala`

Abstract, exactly like the addons one: exposes the reader's inputs as `lazy val` DataFrames and
declares the run method.

```scala
abstract class RunnerProvider(primaryReader: PrimaryReader) extends Serializable {
  private[common] lazy val hdfs_inventory: DataFrame = primaryReader.getMappingReader(PrimaryConstants.HDFS_INVENTORY)
  private[common] lazy val hive_catalog:   DataFrame = primaryReader.getMappingReader(PrimaryConstants.HIVE_CATALOG)
  private[common] lazy val purge_policy:   DataFrame = primaryReader.getMappingReader(PrimaryConstants.PURGE_POLICY)
  private[common] lazy val run_history:    DataFrame = primaryReader.getMappingReader(PrimaryConstants.RUN_HISTORY)
  private[common] lazy val purge_scope:    DataFrame = primaryReader.getMappingReader(PrimaryConstants.PURGE_SCOPE)

  def run_purge_runner(runId: String): PurgeOutcome
}
```

### 7.3 `common/PrimaryRunner.scala`

Assembles the inputs into the candidate set through `PrimaryMapper`, then runs
`PurgeControlMapper` on it, returning `PurgeOutcome(manifest, report)` — the same split as
`tseadfwd`, where the mapper computes and the check mapper only judges.

### 7.4 `reader/PrimaryReader.scala`

Same switch-by-name shape as `com.bnp.str.addons.reader.PrimaryReader`; each input is a `lazy val`
so an unused source is never materialised.

| Input name | Source | Produced by |
|---|---|---|
| `hdfs_inventory` | HDFS recursive listing of the scope roots | `InventoryReader` |
| `hive_catalog` | `SHOW TABLES` / `SHOW PARTITIONS` on the scope databases | `CatalogReader` |
| `purge_policy` | HOCON block, or the `purge_policy` Hive table | `PolicyReader` |
| `run_history` | shared `run_history` ORC table | `RunAuditStore.read` / `readFiles` |
| `purge_scope` | the explicit selection TWIST wrote in the conf | conf → DataFrame |

**`InventoryReader`** is the only genuinely new IO. It walks the scope roots with
`FileSystem.listStatusIterator` (iterator, not `listStatus`, so a directory with 100k entries does
not blow the driver heap), stops at `scan.maxDepth`, and emits one row per *purge unit* — a leaf
directory when the layout is Hive-partitioned, a file otherwise:

```
path, parent_path, object_type, depth, size_bytes, num_files,
modification_time, access_time, owner, group, permission, is_partition_dir
```

The walk is parallelised: roots are `parallelize`d and listed on the executors, so a wide tree does
not serialise through the driver.

### 7.5 `mapping/PrimaryMapper.scala` + `PrimaryView.scala`

Same idiom as addons: register the inputs as temp views, run either the built-in query
(`PrimaryView.get_purge_candidates`) or a query loaded from the SQL file when `queryName` is set,
then clean the result. The built-in query:

- joins `hdfs_inventory` with `hive_catalog` to attach `database` / `table` / `partition_spec` when
  the path is a registered partition;
- joins `purge_policy` on the most specific matching pattern (longest matching prefix wins);
- computes `age_days` from `modification_time`, or from the partition's own date column when the
  policy declares `dateColumn` — which is more truthful than the file mtime (a re-run rewrites the
  file and rejuvenates an old vintage);
- keeps the rows where `age_days >= retention_days`, or where the object is explicitly listed in
  `purge_scope`;
- ranks vintages per `(database, table)` so `keepMinVersions` can be enforced by the controls;
- projects the manifest columns of §9.2.

`ManifestView` computes the **manifest fingerprint** — a SHA-256 over the sorted
`(path, size_bytes, modification_time)` triples — stored on the manifest and re-checked at
execution (control `PC12`).

### 7.6 `control/` — the control framework

Direct transposition of `com.bnp.str.tseadfwd.coherence`:

- `CheckModel.scala` — `PurgeRule(id, title, detail, blocking)`, `PurgeFinding`,
  `CheckRuleResult(rule, enabled, total, findings, objectsBlocked, applied)`,
  `CheckReport(source, runId, generatedAt, candidatesIn, toDelete, blocked, bytesToFree, results)`
  with `verdict`, `summaryLine` and `blockedPaths`. Pure data, no Spark, no IO — unit-testable.
- `CheckConfig.scala` — reads `purge_app.controls { }`: global `enabled`, and per rule
  `enabled` / `blocking` / rule-specific parameters.
- `PurgeControlMapper.scala` — takes the candidate DataFrame, evaluates every enabled rule, and
  returns `ControlOutcome(manifest, report)` where `manifest` carries two extra columns:
  `decision` (`DELETE` / `DELETE_WITH_WARNING` / `BLOCKED`) and `controls_ko` (array of rule ids).
- `CheckHtmlView.scala` / `CheckWriter.scala` — the HTML report, written next to the manifest and
  linked from TWIST.

### 7.7 `purge/` — the deletion

`PurgeStrategy` is a sealed trait with four cases:

| Strategy | Implementation | Recoverable | Default use |
|---|---|---|---|
| `TRASH` | `Trash.moveToAppropriateTrash(fs, path, conf)` | Yes, during `fs.trash.interval` | **Default** for every path |
| `HARD` | `fs.delete(path, recursive = true)` | No | Only when Trash is disabled or over quota, and an archive exists |
| `DROP_PARTITION` | `ALTER TABLE … DROP PARTITION` (+ path handling per `external.table.purge`) | Metadata only | Registered Hive partitions |
| `LOGICAL` | insert into a `purge_flag` table, nothing removed | n/a | Tables whose readers cannot tolerate a missing partition |

`PurgeExecutor` iterates the manifest rows with `decision != BLOCKED` **on the driver** (deletion is
a metadata operation on the NameNode; fanning it out to executors buys nothing and makes failure
handling opaque), and records one `PurgeDetailRecord` per object. It is idempotent: a path already
gone is recorded `SKIPPED_ABSENT`, not `FAILED`. A per-object failure does not abort the run; the
run ends `PARTIAL`.

`PurgeGuard` is the last, **hard-coded**, non-configurable safety net, asserted before a single
delete. It refuses the whole run — not just the offending row — when any of these is true:

- a target path is not under one of `purge_app.scan.allowedRoots`;
- a target path has fewer than `MIN_PATH_DEPTH` (= 4) segments, or matches the denylist
  (`/`, `/user`, `/tmp`, `/apps`, `/warehouse`, any Hive database root, any unresolved `*`);
- the run would delete more than `guard.maxBytes` / `guard.maxObjects` without a second approval;
- `mode = EXECUTE` and the approval token is missing or does not match the manifest fingerprint.

**Dropping a whole table.** A table-granular run — the classic simulator, where a run IS its tables —
produces manifest rows whose `partition_spec` is empty and which carry `is_registered_table = true`,
a column the selection SQL sets when a candidate's path is EXACTLY a registered table's location.
`PurgeExecutor.maybeDropTable` then drops the table after its data has gone, the same way
`maybeDropPartition` drops a partition, and `execution.dropHiveTable` (default `true`) switches it
off for a perimeter that wants the data removed and the definition kept. Two safeguards sit around
it: a row carrying a partition spec is never dropped as a table even if it claims to be one, and the
partition drop is tried first, so the more destructive instrument is only ever reached when the less
destructive one did not apply.

This duplicates part of the controls on purpose: the controls are configurable and can be switched
off; `PurgeGuard` cannot.

### 7.8 `audit/`

- `PurgeAudit.scala` — module wiring on the shared `RunAudit`, in the shape of `TseadfwdAudit`:
  `MODULE_NAME = "purge"`, `projection_dates` ← the as-of / date filter, `scenarios` ← the scope
  roots as a JSON-ish list, `base_folder_name` ← the request id.
- `PurgeDetailStore.scala` — mirrors `RunAuditStore`: an EXTERNAL ORC Hive table
  `purge_detail`, `PARTITIONED BY (purge_date STRING, run_id STRING)`, written with the ORC
  datasource (no metastore needed) and registered best-effort.

### 7.9 `utility/PrimaryConstants.scala`

```scala
object PrimaryConstants {
  val APPLICATION_NAME = "purge_engine"
  val APP_CONF         = "purge_app"

  // input identifiers (config keys + PrimaryReader switch)
  val HDFS_INVENTORY = "hdfs_inventory"
  val HIVE_CATALOG   = "hive_catalog"
  val PURGE_POLICY   = "purge_policy"
  val PURGE_SCOPE    = "purge_scope"
  val RUN_HISTORY    = "run_history"

  // temp-view names shared by the mapper and the selection SQL
  val VIEW_HDFS_INVENTORY = "hdfs_inventory_view"
  val VIEW_HIVE_CATALOG   = "hive_catalog_view"
  val VIEW_PURGE_POLICY   = "purge_policy_view"
  val VIEW_PURGE_SCOPE    = "purge_scope_view"
  val VIEW_RUN_HISTORY    = "run_history_view"

  // outputs
  val PURGE_MANIFEST = "purge_manifest"
  val PURGE_DETAIL   = "purge_detail"

  // decisions
  val DECISION_DELETE  = "DELETE"
  val DECISION_WARN    = "DELETE_WITH_WARNING"
  val DECISION_BLOCKED = "BLOCKED"

  // guard — NOT configurable
  val MIN_PATH_DEPTH  = 4
  val FORBIDDEN_PATHS = Set("/", "/user", "/tmp", "/apps", "/warehouse")
}
```

---

## 8. The control framework

Every candidate goes through every enabled rule. A `BLOCKING` rule that fires sets the candidate to
`BLOCKED` — it will not be deleted, whatever the approval says. A `WARNING` rule only annotates it.

| Id | Rule | Type | What it checks |
|---|---|---|---|
| **PC01** | `RETENTION_NOT_REACHED` | BLOCKING | A policy matched and its retention is not reached. An object **no policy matched is not blocked here** — see PC15 and §16.Q3. `blockWithoutPolicy = true` restores the stricter default-deny reading. |
| **PC02** | `LEGAL_HOLD` | BLOCKING | The domain / table / perimeter carries a legal or regulatory hold (`legal_hold = true`, or an open freeze window). Regulatory retention wins over any purge request. |
| **PC03** | `PATH_OUT_OF_SCOPE` | BLOCKING | The path is not under an allowed root, is too shallow, or matches the denylist. Also fires on a path escaping its declared root via `..` or a symlink. |
| **PC04** | `ACTIVE_RUN` | BLOCKING | `run_history` holds a `RUNNING` row whose output touches this path/table, or a run finished less than `minQuietMinutes` ago. Never delete under a live writer. |
| **PC05** | `DOWNSTREAM_DEPENDENCY` | BLOCKING | The table/partition feeds a downstream object declared in the dependency referential (or discovered from `run_history` inputs). Deleting it would break a consumer. |
| **PC06** | `MIN_VERSIONS_KEPT` | BLOCKING | The deletion would leave fewer than `keepMinVersions` vintages for that `(database, table)`, or would remove the last `SUCCESS` delivery. |
| **PC07** | `NO_ARCHIVE` | BLOCKING | The policy demands an archive before a `HARD` delete and no archive copy is found at the declared archive root. Not evaluated for `TRASH`. |
| **PC08** | `NOT_AUTHORIZED` | BLOCKING | The requester is not owner of the perimeter, or not a member of the purge group for that domain. |
| **PC09** | `BLAST_RADIUS` | BLOCKING | The run exceeds `guard.maxBytes` / `maxObjects` / `maxPercentOfTable`. Releasable only by a second, explicit approval. |
| **PC10** | `RECENTLY_ACCESSED` | WARNING | HDFS `atime` within `recentAccessDays`. Someone is still reading it — worth a human look, not a veto (atime is unreliable on some clusters). |
| **PC11** | `HIVE_METADATA_ORPHAN` | WARNING | The path is a registered Hive partition, or a registered table in its own right: deleting the files alone would leave orphan metadata. The executor is instructed to `DROP PARTITION` / `DROP TABLE` as well. |
| **PC12** | `MANIFEST_DRIFT` | BLOCKING (execute phase) | At execution the object's `(size_bytes, modification_time)` no longer matches the manifest, or the fingerprint does not match the approval token. The world changed since the simulation — abort. |
| **PC13** | `ALREADY_ABSENT` | WARNING | The path no longer exists. Recorded `SKIPPED_ABSENT`, keeps the run idempotent. |
| **PC14** | `SHARED_INPUT` | BLOCKING | The object is declared as an INPUT by one of the runs being purged, or is the engine's own run history — or is a directory containing one. Inputs are shared across runs and belong to none of them. |
| **PC15** | `NO_RETENTION_POLICY` | WARNING | No policy in the referential governs this object, so nothing but the explicit selection decides whether it may go. Reported so the fact stays visible after PC01 stopped blocking it. |

Each rule is individually switchable — except `PC03`, `PC09` and `PC12`, which `PurgeGuard`
re-checks in code and which therefore cannot be disabled: a report that omitted them would describe
a run whose real behaviour is stricter than what it says.

**A control that could not run is never reported as PASS.** The report distinguishes four
non-verdicts, and the distinction is most of its value — "we checked and found nothing" and "we
could not check" lead to very different decisions:

| Status | Meaning |
|---|---|
| `PASS` | Evaluated, nothing found. |
| `SKIPPED` | Switched off in `purge_app.controls`. |
| `NOT EVALUATED` | Belongs to the execution phase (`PC12`, `PC13`); a simulation has nothing to evaluate it against. |
| `UNKNOWN` | The data the control needs was not available — an empty `run_history` for `PC04`, no dependency referential for `PC05`, no requester groups for `PC08`. |

`UNKNOWN` controls are listed in a banner at the top of the HTML report, above the counters and the
matrix: an approver reading a green report has to know which greens were actually checked before
reading anything else.

The report renders, per rule: status (`PASS` / `SKIPPED` / `BLOCKED(n)` / `WARNED(n)`), the count,
a capped list of offending objects, and the action taken — the same `status` / `action` logic as
`CheckRuleResult` in `tseadfwd`.

---

## 9. Data model

### 9.1 `purge_request` — owned by TWIST

`request_id`, `requester`, `domain`, `scope_json`, `mode`, `status`, `simulation_run_id`,
`manifest_fingerprint`, `approver`, `approval_token`, `approval_date`, `execution_run_id`,
`created_at`, `updated_at`, `comment`.

The **approval token** is `HMAC(secret, request_id + manifest_fingerprint + approver)`. The engine
recomputes it; a manifest edited between approval and execution cannot be executed.

### 9.2 `purge_manifest` — written by the simulation

ORC, `PARTITIONED BY (purge_date, run_id)`.

```
run_id, request_id, object_type, path, database_name, table_name, partition_spec,
size_bytes, num_files, modification_time, access_time, owner_name,
business_date, business_date_source, age_days, selection_source,
source_engine, source_run_id,
policy_id, retention_value, retention_unit, retention_cutoff, keep_min_versions,
strategy, legal_hold, archive_required, archive_root, owner_group,
version_group, version_rank, is_registered_partition, external_purge,
decision, controls_ko, controls_warn, manifest_fingerprint, generated_at
```

### 9.2bis Where the audit lives: `dbpurge`

`purge_detail` and the purge engine's `run_history` live in **`dbpurge`**, a database of the purge
engine's own, and not in the database being purged.

The reason is the one that governs everything else here: an audit that sits inside its subject is an
audit a wide enough purge can take with it. `dbpurge` is never any engine's output, so nothing an
engine-driven scope expands to can reach it, and `allowedRoots` can exclude it outright. It also
gives the IHM's history screen and any auditor a single place to look, whichever engine a run was
about. The tables keep `external.table.purge = FALSE` (§14ter), so even `DROP TABLE` on them leaves
the ORC behind.

### 9.3 `purge_detail` — written by the execution

ORC external Hive table, `PARTITIONED BY (purge_date, run_id)` — the object-level twin of
`run_history`.

```
run_id, request_id, path, database_name, table_name, partition_spec, strategy,
status, bytes_freed, num_files, trash_path, restore_deadline, error_message,
executed_at, user_launcher, source_engine, source_run_id
```

`source_engine` / `source_run_id` name the engine run each object belonged to, carried from the
scope through the manifest. Without them "what happened to run X?" is answerable only at run level,
through `run_history.scenarios` — and for a table-granular engine, whose rows carry no partition
spec and whose table names carry no run id, not per object at all. They are appended at the end of
the table, so a `purge_detail` registered by an earlier version needs only
`ALTER TABLE … ADD COLUMNS`.

`status ∈ { DELETED, TRASHED, PARTITION_DROPPED, TABLE_DROPPED, LOGICALLY_DELETED, SKIPPED_BLOCKED,
SKIPPED_ABSENT, SKIPPED_DRIFT, SKIPPED_ABORTED, FAILED }`.

### 9.4 `run_history` — reused as-is

One row per purge run, `module_name = 'purge'`, through the existing
`com.bnp.str.utilities.audit.RunAudit`. No change to the shared code; `motor = 'purge'`,
`base_folder_name = request_id`.

### 9.5 `purge_policy` — the retention referential

```
policy_id, domain, path_pattern, database_name, table_pattern, date_column,
retention_value, retention_unit (DAY|MONTH|QUARTER|YEAR), keep_min_versions,
strategy, legal_hold, archive_required, archive_root, owner_group,
valid_from, valid_to, created_by, created_at
```

Matching is **longest-pattern-wins**, and a candidate matching no policy is `BLOCKED` by `PC01`
(default-deny): absence of a rule is never a licence to delete.

---

## 10. Configuration — `application.conf`

Same HOCON conventions as the other engines (a single root block named after the app, `enable`
flags on outputs, an `audit { }` block).

```hocon
purge_app {

  # ---- what this run is ----
  request {
    id            = "PRG-2026-000142"
    mode          = "SIMULATE"          # SIMULATE | EXECUTE | SIMULATE_AND_EXECUTE
    requester     = "j03627"
    approver      = ""                  # filled by TWIST at approval
    approvalToken = ""                  # HMAC(request_id + manifest_fingerprint + approver)
    manifestRunId = ""                  # EXECUTE: the simulation run whose manifest is replayed
    comment       = "Purge des vintages 2023 du domaine RWA"
  }

  # ---- WHICH ENGINE, and WHICH RUN of it ----
  engine {
    name         = "projection"
    # the configuration(s) of the run(s) to purge - TWIST writes one entry per run the user selected.
    # Everything else comes out of that file, including the database (output.database.name).
    runConfPaths = [ "/Projects/.../Projection/conf/9df8cf3a.properties" ]
  }

  # ---- WHICH ENGINE, and WHICH RUN of it ----
  engine {
    name         = "projection"
    # the configuration(s) of the run(s) to purge - TWIST writes one entry per run the user selected.
    # Everything else comes out of that file, including the database (output.database.name).
    runConfPaths = [ "/Projects/.../Projection/conf/9df8cf3a.properties" ]
  }

  # ---- what to look at ----
  scan {
    allowedRoots   = [ "/data/promethee/str/rwa", "/data/promethee/str/ifrs9" ]
    roots          = [ "/data/promethee/str/rwa/ts_ead_fwd" ]
    databases      = [ "dbiris" ]
    tables         = [ "ts_ead_fwd", "crr_param_add_on_ste" ]
    maxDepth       = 6
    followSymlinks = false
    parallelism    = 32
  }

  # ---- explicit selection coming from the IHM (optional; empty = policy-driven run) ----
  purge_scope {
    enable     = true
    paths      = [ "/data/promethee/str/rwa/ts_ead_fwd/as_of_date=2023-03-31" ]
    partitions = [ { database = "dbiris", table = "ts_ead_fwd", spec = "as_of_date=2023-06-30" } ]
  }

  # ---- retention referential: inline, or a Hive table ----
  purge_policy {
    source = "conf"                     # conf | table
    table  = "dbiris.purge_policy"
    rules = [
      { policyId = "P-RWA-ORC", domain = "RWA", pathPattern = "/data/promethee/str/rwa/*",
        dateColumn = "as_of_date", retentionValue = 8, retentionUnit = "QUARTER",
        keepMinVersions = 4, strategy = "TRASH", legalHold = false, archiveRequired = false,
        ownerGroup = "grp_str_rwa" },
      { policyId = "P-IFRS9-REG", domain = "IFRS9", pathPattern = "/data/promethee/str/ifrs9/*",
        dateColumn = "as_of_date", retentionValue = 10, retentionUnit = "YEAR",
        keepMinVersions = 8, strategy = "HARD", legalHold = true, archiveRequired = true,
        archiveRoot = "/archive/promethee/str/ifrs9", ownerGroup = "grp_str_ifrs9" }
    ]
  }

  # ---- controls ----
  controls {
    enabled            = true
    maxFindingsPerRule = 200
    htmlPath           = "/data/promethee/str/purge/reports/PRG-2026-000142.html"

    retention_not_reached { enabled = true }
    legal_hold            { enabled = true }
    path_out_of_scope     { enabled = true }                       # blocking, cannot be switched off
    active_run            { enabled = true, minQuietMinutes = 60 }
    downstream_dependency { enabled = true, referential = "dbiris.data_dependency" }
    min_versions_kept     { enabled = true }
    no_archive            { enabled = true }
    not_authorized        { enabled = true }
    blast_radius          { enabled = true }
    shared_input          { enabled = true }
    shared_input          { enabled = true }
    recently_accessed     { enabled = true, recentAccessDays = 30, blocking = false }
    hive_metadata_orphan  { enabled = true, blocking = false }
    manifest_drift        { enabled = true }
    already_absent        { enabled = true, blocking = false }
  }

  # ---- hard safety net (PurgeGuard) ----
  guard {
    maxBytes            = 5497558138880          # 5 TiB
    maxObjects          = 50000
    maxPercentOfTable   = 50
    requireApproval     = true
    manifestMaxAgeHours = 72
  }

  # ---- deletion ----
  execution {
    defaultStrategy   = "TRASH"
    dropHivePartition = true
    stopOnFirstError  = false
    logEveryNObjects  = 100
  }

  # ---- outputs ----
  purge_manifest {
    enable        = true
    format        = "orc"
    path          = "/data/promethee/str/purge/manifest/"
    tmpPath       = "/data/promethee/str/purge/manifest_tmp/"
    tableName     = "purge_manifest"
    numPartition  = 1
    mode          = "Overwrite"
    deleteTmpPath = "true"
    queryName     = ""
  }

  purge_detail {
    enable   = true
    database = "dbpurge"                 # the purge engine's own database, never an engine's output
    table    = "purge_detail"
    root     = "/data/promethee/str/purge/purge_detail"
  }

  sql_queries { path = "/data/promethee/str/purge/sql/queries.sql.conf" }

  # ---- shared run audit (module_name = purge) ----
  audit {
    enabled      = true
    database     = "dbpurge"
    table        = "run_history"
    root         = "/data/promethee/str/run_history"
    userLauncher = "j03627"
    motor        = "purge"
  }
}
```

---

## 11. The TWIST IHM

The purge IHM is a new TWIST module. It reuses the existing launch plumbing (conf generation +
`spark-submit` + log streaming + run history) and adds six screens.

**The division of work.** Every screen below is TWIST's to build. What the engine owes each one — a
driver to submit and an output to read — is listed here and is implemented, except where marked P6.
The engine exposes no REST API and holds no session: a screen submits a `spark-submit` and reads
ORC, exactly as TWIST already drives `addons`, `ageing` and `tseadfwd`.

| Screen | Submits | Reads |
|---|---|---|
| S1 Périmètre | `RunCatalogDriver` | `purge_run_catalog` |
| S2 Analyse | `SimulationDriver` | `purge_manifest`, the HTML report at `controls.htmlPath` |
| S3 Confirmation | — | — |
| S4 Exécution | `MainDriver` (`mode = EXECUTE`, `manifestRunId`) | `purge_detail` |
| S5 Historique | — | `run_history` (`module_name = 'purge'`) ⋈ `purge_detail` |
| S6 Restauration | `RestoreDriver` *(P6)* | `purge_detail.trash_path`, `restore_deadline` |

### The run catalogue, and why it exists

A purge names a run, so a person must choose one from a list — and until P5 nothing produced that
list: the engine had to be handed a run's configuration before it could say anything at all. Asking
TWIST to build the list instead would mean asking it to walk HDFS, which is the work this engine
exists to do, and would give two different answers to "how big is that run?".

`RunCatalogReader` is built from the FILESYSTEM and then annotated from the engine's history, in that
order and never the reverse. What is on disk is what a purge would remove; the history says what the
engine believes it produced. Where they disagree, the disagreement is the interesting part:

- **on disk, absent from the history** — a run the engine lost track of, and the likeliest thing
  anyone actually wants to purge;
- **in the history, absent from disk** — already purged, or never written.

A catalogue built from the history alone would show the second and miss the first. `S1` should
therefore surface `known_to_history = false` rather than bury it.

### S1 — Scope selection ("Périmètre")

Two entry modes, exclusive:

- **By policy** — pick a domain; the screen shows what the retention policy would make eligible
  today. This is the normal, governed path.
- **By selection** — browse a tree `Domain → Database → Table → Partitions` (fed from the Hive
  metastore) or an HDFS path browser restricted to `allowedRoots`. Multi-select, with filters on
  as-of date / quarter, age, size, owner, producing engine (`module_name` from `run_history`).

Each node shows size, number of files, number of partitions, age of the newest vintage, matched
policy, and a **traffic light** — green (eligible), orange (eligible with warnings), red (blocked),
grey (no policy → default-deny). The light is indicative until the simulation runs.

### S2 — Simulation ("Analyse")

One button. TWIST writes the conf with `mode = SIMULATE` and submits `SimulationDriver`. Live log,
then a result panel:

- headline: *N objects analysed · X to delete · Y blocked · Z TB freed*;
- the control matrix (one line per rule: status, count, action);
- the candidate table, sortable and filterable, with the per-object `decision` and `controls_ko`;
- a link to the HTML report and a CSV export of the manifest.

Nothing has been deleted. The request moves to `SIMULATED`.

### S3 — Confirmation ("Confirmation")

This screen was four-eyes approval. The business decided against an approval step for this version
(open question Q11): the business user holds the purge role and owns the data. What remains is one
person confirming deliberately.

- Confirmation requires re-typing the request id.
- The screen names the restore window — **7 days** — at the moment of the click, not in a help page.
  That window is the whole of what "reversible" means here.
- Blocked objects are shown but cannot be released from this screen: a blocking control is lifted by
  fixing its cause, never by clicking through.
- `guard.requireApproval = true` re-enables the four-eyes check in `PurgeGuard` (approver ≠
  requester, and a token carrying the manifest fingerprint). It is implemented and inert, so a
  sensitive perimeter can switch it back on with a configuration change rather than a code change.

### S4 — Execution ("Exécution")

TWIST writes the conf with `mode = EXECUTE`, `manifestRunId` and `approvalToken`, and submits
`MainDriver`. Progress driven by `purge_detail` rows. Final panel: objects deleted, bytes freed,
failures with their error, restore deadline. A **Cancel** stops after the current object;
already-deleted objects stay in `purge_detail` and remain restorable from Trash.

### S5 — History ("Historique")

`run_history` (module `purge`) joined with `purge_detail`: filter by requester, domain, date,
status; drill down to the object list; re-open the HTML report of any past run. Read-only and
exportable — this is the screen an auditor is shown.

### S6 — Restore ("Restauration")

For a run still within its trash window: select objects, TWIST submits `RestoreDriver`, which moves
them back from `trash_path` to `path` and re-registers the Hive partition when there was one.
Objects past `restore_deadline` are shown greyed, with the deadline that expired.

### Cross-cutting IHM rules

- Any screen action that changes state writes to `purge_request` and is itself audited (who, when,
  from which screen).
- The IHM never issues an HDFS command itself. Every deletion goes through the engine, so there is
  exactly one code path to review and one audit trail.
- Identity: the engine runs under a dedicated service account holding the HDFS ACLs to delete, and
  the requester's identity travels as `user_launcher`. Where the cluster allows it, prefer
  `--proxy-user <requester>` so HDFS itself enforces the requester's rights and the service account
  cannot become a universal delete key.

---

## 12. Safety guarantees — summary

| # | Guarantee | Enforced by |
|---|---|---|
| 1 | Nothing is deleted without a prior simulation | `mode` + `manifestRunId` required in EXECUTE |
| 2 | Only what is in the approved manifest is deleted | `PurgeExecutor` reads the manifest, never re-scans for targets |
| 3 | The manifest cannot be tampered with between approval and execution | fingerprint + HMAC approval token (`PC12`) |
| 4 | Nothing is deleted outside the declared roots | `scan.allowedRoots` (`PC03`) **and** `PurgeGuard` |
| 5 | No catastrophic path | `MIN_PATH_DEPTH`, `FORBIDDEN_PATHS`, hard-coded |
| 6 | No deletion under a live writer | `PC04` against `run_history` |
| 7 | Bounded blast radius | `PC09` + `guard.maxBytes` / `maxObjects` |
| 8 | Recoverable by default | `TRASH` strategy + `RestoreDriver` |
| 9 | Two humans, not one | 4-eyes in TWIST, server-side |
| 10 | Fully auditable | `run_history` + `purge_detail` + HTML report, immutable ORC |
| 11 | Default-deny | a candidate matching no policy is blocked, not allowed |
| 12 | Idempotent | an absent path is `SKIPPED_ABSENT`; re-running a manifest is safe |

---

## 13. Testing strategy

Mirrors the existing test layout (`SparkTestSession`, `*Spec` with ScalaTest +
`spark-testing-base`).

| Spec | Covers |
|---|---|
| `PrimaryUtilitiesSpec` | path helpers, size formatting, HDFS reader, fingerprint |
| `DateUtilsSpec` | age computation across DAY/MONTH/QUARTER/YEAR, quarter parsing |
| `PrimaryViewSpec` | candidate-selection SQL on a synthetic inventory (policy matching, longest-prefix, version ranking) |
| `PurgeControlMapperSpec` | one test per rule: fires / does not fire / disabled → SKIPPED; decision column |
| `PurgeGuardSpec` | every guard refusal: path outside root, depth < 4, token mismatch, over-volume |
| `EngineGranularitySpec` | the two granularities expand to DIFFERENT scopes, PC04's token follows the granularity, an unknown value is refused, and executing a table-granular run is refused |
| `PurgeExecutorSpec` | against a local temp FS: TRASH moves and keeps the file, absent path → `SKIPPED_ABSENT`, one failure does not abort the run |
| `CheckHtmlViewSpec` | golden-file rendering of the report |
| `PurgeAuditSmokeTest` | `run_history` + `purge_detail` write on the local Derby / ORC path |

Plus a **rehearsal on the cluster**: a dedicated `/data/promethee/str/purge/sandbox` tree, purged
end-to-end through TWIST before the first real perimeter is opened.

---

## 14. Delivery phases

| Phase | Content | Deliverable |
|---|---|---|
| **P0** | This design, reviewed with the team | spec validated |
| **P1** | Project skeleton (pom, packages), `StrSparkSessionManager`, `PrimaryConstants`, `PrimaryUtilities`, `DateUtils`, `InventoryReader`, `CatalogReader`, `PrimaryWriter`, and `InventoryDriver` — a read-only entry point that exists only until `MainDriver` lands at P4 | the engine can inventory a scope and write it as ORC |
| **P2** | `PolicyReader`, `PrimaryReader`, the `common` runner trio, `PrimaryMapper` / `PrimaryView` / `ManifestView`, and `SimulationDriver` | simulation produces a fingerprinted manifest |
| **P3** | `control/` — `CheckModel`, `CheckConfig`, the 15 rules in `PurgeControlMapper`, `CheckHtmlView`, `CheckWriter`; `decision` and `controls_ko` filled in | simulation produces a signed-off report — **still zero deletion** |
| **P4** | `purge/` — `PurgeStrategy`, `PurgeGuard`, `PurgeExecutor`; `audit/` — `PurgeAudit`, `PurgeDetailStore`; `writer/PurgeDetailWriter`, `reader/ManifestReader`, `job/MainDriver` | **done** — executes on the fixture perimeter, end to end |
| **P5** | TWIST S1 → S5 | end-to-end from the IHM |
| **P6** | `RestoreDriver` + S6, policy referential CRUD, scheduled mode | run |

P1 → P3 are safe by construction (nothing deletes), so they can go to the cluster early and the
retention policies can be validated against real data before any destructive code exists.

---

## 16. What the business answered, and what it changed

The twelve questions of §15 were answered on **2026-08-29**. The workbook
`docs/purge/OPEN_QUESTIONS_PURGE.xlsx` carries each reply verbatim beside the decision it produced;
this section records only what the answers changed in the design.

### The model shifted: business-owned, not policy-governed

Three answers move together and are the most consequential thing in this document.

- **Q3 — retention policies will not be handled.** The business owns its data and decides which runs
  to purge. There is no automatic ageing-out.
- **Q11 — no approval step in this version.** The business user holds the purge role and owns the
  data; there is no second signature.
- **Q2 — the business alone holds the right to purge**, and knows the regulatory rules that apply.

The design was built around the opposite assumption: a retention policy authorises a deletion, and a
second person approves it. Both gates are gone, so **what protects data is no longer "a rule allowed
it" but "someone named it"** — and naming is exactly what an engine run scope is (§3bis): the objects
in play are read from the engine's own run configuration, and nothing else is selected.

That is a real reduction in safety, and it is worth being explicit about which guarantees survive it:

| Guarantee | After the answers |
|---|---|
| Nothing deleted without a prior simulation | **Kept.** Two-phase SIMULATE → EXECUTE is what lets a person see exactly what will go before it goes; that is worth having with or without a second signature. |
| Only what is in the manifest is deleted | **Kept.** |
| The manifest cannot change under you | **Kept.** PC12 protects against the filesystem moving, not against a person, so it survives the loss of the approval token. |
| Containment (`allowedRoots`, depth, denylist) | **Kept, and now load-bearing.** With no proxy-user (Q6) the service account can reach more than the requester could, so PC03 and PurgeGuard are most of what stands in the way. |
| Bounded blast radius | **Kept**, but PC09 now simply aborts: there is no escalation path to release a run that exceeds its ceilings. |
| Reversible by default | **Kept, with a stated limit of 7 days** (Q5). |
| Two humans, not one | **Dropped for v1** (Q11). |
| Default-deny by policy | **Replaced** by default-deny by scope (Q3). |

Concretely, in the code:

- **PC01** now fires only when a policy matched *and* its retention is not reached. Left as it was,
  it would have blocked every purge — with no policies, every object matched none.
- **PC15 `NO_RETENTION_POLICY`** was added as a warning so the information PC01 used to carry does
  not vanish: the report still says "nothing but your own selection is protecting this object".
- **`retention_not_reached.blockWithoutPolicy`** makes the choice visible at the point of use. It
  defaults to `false` per Q3; a perimeter that wants the stricter reading sets it to `true`.

### Corrections to stated assumptions

- **Q8 — `external.table.purge = TRUE`, the opposite of what §15.7 assumed.** `DROP PARTITION`
  therefore deletes the data itself: for a registered partition the metadata drop *is* the deletion,
  and the P4 executor must not also delete the path. **To verify before P4:** whether a Hive-driven
  drop honours Trash — if it does not, the 7-day window of Q5 does not cover that path, and the
  strategy for registered partitions has to change.
- **Q6 — service account, not `--proxy-user`.** The design's preference was not taken. The requester
  is carried in the configuration for traceability only, and HDFS will not enforce their rights.
- **Q1 — one engine, the simulator, stores a run as a WHOLE TABLE** rather than as a partition.
  `EngineDescriptor` gained a `granularity` (`PARTITION` | `TABLE`); projection declares `PARTITION`,
  and the simulator descriptor will declare `TABLE` when it is added. Getting this wrong in either
  direction is severe — dropping a table where a partition was meant, or the reverse.

  **The field is now load-bearing, which it was not when it was first added.** It was declared,
  documented and read by nothing: every run expanded to `<table>/runId=<uuid>` whatever it said, so
  the first table-granular descriptor would have produced a scope of directories that do not exist
  and a purge that quietly did nothing. Granularity now decides the scope shape
  (`EngineRun.relativePathOf` / `partitionSpecOf`, §3bis), the run catalogue's run-id recovery, and
  PC04's path token — and an unknown value is refused where the descriptor is resolved rather than
  falling through to the partition reading, which is the more destructive of the two to get wrong.
  The deletion followed: `PurgeExecutor.maybeDropTable` drops a whole table once its data is in
  Trash (§7.7), so `PurgeGuard` no longer has to refuse a table-granular run. The first engine of
  that shape — the classic simulator — is described in
  `docs/purge/simulator_classic_analysis.md`.
- **Q10 — there is one partition level, not two.** The double nesting the layout screenshot hinted
  at does not exist; the inventory needs no change, since it walks to the leaf whatever the depth.

### Confirmations, no change needed

- **Q9** every projection output is declared in the run configuration — the MVP's premise holds.
  The same confirmation is needed per engine as each is added.
- **Q12** the purge engine is its own artifact, dedicated to delete and purge operations.
- **Q4** no lineage referential yet; it will be built incrementally *using* this project. PC05 keeps
  reporting UNKNOWN until it exists.
- **Q7** archive is on hold and may live outside Cloudera, beyond a Spark job's reach. PC07 stays
  implemented and inert.
- **Q5** Trash is enabled and emptied after 7 days, which fixes the restore window.

---

## 14bis. How the execution works

P4 turned the design into code. Four decisions inside it are not obvious from the phase table.

### Trash first, drop second

Every STR output is an EXTERNAL table with `external.table.purge = TRUE` (Q8), so a bare
`DROP PARTITION` deletes the data **through Hive** — outside this engine's deletion path, and with
no certainty that it honours Trash. Rather than answer that question, the executor sidesteps it:

```
1. move the data to Trash          -> recoverable for fs.trash.interval (7 days)
2. ALTER TABLE ... DROP PARTITION  -> the location is already empty, so this is pure metadata
```

`TRASH` and `DROP_PARTITION` therefore take the same path. The metastore ends up consistent, the
7-day window still applies, and the open verification item about Hive and Trash stops mattering.

The same ordering, for the same reason, applies one level up. **The business confirmed on 2026-09-07
that `external.table.purge = TRUE` holds for the projection AND the simulator tables**, so
`DROP TABLE` would also delete data through Hive. A table-granular object is therefore trashed first
and dropped second, and the drop is again pure metadata against an empty location. Nothing about the
restore window changes between purging a partition and purging a table.

### Trash unavailable is a failure, not a fallback

`moveToAppropriateTrash` returns false when `fs.trash.interval` is 0 and leaves the data in place.
Treating that as success would report a purge that did not happen; falling back to a hard delete
would destroy the recoverability the strategy was chosen for. So the object **fails** and says why,
and the run ends `PARTIAL`. `execution.failWhenTrashUnavailable = false` opts out, deliberately
awkwardly.

### Drift is measured the way the inventory measured

A leaf directory's size is the sum of the files it holds and its modification time is the newest
among itself and those files — that is what the inventory records, so that is what the executor
compares against. Comparing instead against the directory's own `getLen` and mtime makes nearly
every partition look changed, and a false drift is not a harmless caution: the object is skipped,
the run reports SUCCESS, and the purge quietly did not happen. This was a real bug, caught by the
first end-to-end execution.

### Every manifest row produces exactly one detail row

`purge_detail` is a complete account of the manifest, not a list of successes. A blocked row is
`SKIPPED_BLOCKED`, an absent one `SKIPPED_ABSENT`, a changed one `SKIPPED_DRIFT`, one the run never
reached `SKIPPED_ABORTED`. That is what makes replaying a manifest safe, and replaying is how a
`PARTIAL` run is finished: everything already removed comes back `SKIPPED_ABSENT`.

| Status | Meaning |
|---|---|
| `TRASHED` | Moved to Trash. Recoverable until `restore_deadline`. |
| `DELETED` | Hard-deleted. Not recoverable. |
| `PARTITION_DROPPED` | Data trashed and the Hive partition dropped. |
| `TABLE_DROPPED` | Data trashed and the Hive table dropped — a table-granular run. |
| `LOGICALLY_DELETED` | Recorded as purged; the data was left in place. |
| `SKIPPED_BLOCKED` | A control blocked it, or the controls never judged it. |
| `SKIPPED_ABSENT` | Already gone. |
| `SKIPPED_DRIFT` | Changed since the simulation. |
| `SKIPPED_ABORTED` | The run stopped before reaching it. |
| `FAILED` | Attempted and failed; the message says why. |

### The modes

`request.mode` is `SIMULATE` (the default, deletes nothing), `EXECUTE` (replays
`request.manifestRunId`, builds nothing), or `SIMULATE_AND_EXECUTE` (both in one submission).
A mistyped mode is refused rather than interpreted — neither silently EXECUTE nor silently SIMULATE.
`SimulationDriver` still exists so TWIST's "Analyse" calls a class with no destructive code at all,
rather than this one in a safe mode.

---

## 14ter. What `dbprojection.run_history` actually looks like

The projection engine's own history was captured from Hue (`docs/purge/run_history_row_detail.md`)
and settles several things this engine had been reading defensively.

```sql
CREATE EXTERNAL TABLE `dbprojection`.`run_history`(
  `run_id` string, `application_id` string, `used_jar` string, `used_conf` string,
  `used_worfklow` string, `user_launcher` string, `creation_date` timestamp,
  `end_date` timestamp, `duration` string, `motor` string, `launch_type` string,
  `run_type` string, `real_user_id` string, `status` string)
TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL'='TRUE', 'external.table.purge'='TRUE', ...)
```

### It changed PC04

`status` holds **`succeeded`** — lower case — and nothing documents what it holds while a run is in
flight. PC04 matched the literal `"RUNNING"`, so against the real table it might simply never have
fired: the one control that stops a purge deleting under a live writer, silently inert.

It now keys off **`end_date IS NULL`** instead. A run with no end has not finished, whatever word its
engine uses, and the column is a real `timestamp` rather than free text. Two bounds go with it:

- an unfinished row is only treated as live while `creation_date` is within
  `active_run.maxRunHours` (24h). Nothing writes an end date for a job whose JVM was killed, and
  without the bound those rows would protect their run for ever — while the runs nobody finished are
  exactly the ones people want to purge;
- the status-word check survives only as a fallback for a history table carrying no `end_date` at
  all, against a list of terminal words rather than a guess at the running one.

### It closed a loop in the IHM

`used_conf` records the path of the `conf.properties` each run was launched with — which is exactly
what a purge of that run needs. The run catalogue now carries it, so S1 can hand S2 the run's
configuration directly instead of TWIST tracking that mapping itself and keeping it in step.

The catalogue also gained `run_type`, `real_user_id`, `started_at`, `ended_at` and `unfinished` from
the same table.

### Facts worth keeping in view

| Fact | Consequence |
|---|---|
| `run_history` is **not partitioned** | The `runId=` partition question is confined to the result tables. |
| `run_id` has **no uniqueness constraint**, and the same id was seen with two `application_id`s | The catalogue aggregates per run id: newest row for the descriptive fields, and `unfinished` true if ANY row lacks an end — the reading that errs towards not deleting. |
| `TRANSLATED_TO_EXTERNAL='TRUE'` with `external.table.purge='TRUE'` | These tables look external and are not drop-safe: `DROP TABLE` deletes the data. Confirms Q8, and confirms that `purge_detail` must set `external.table.purge='FALSE'` on itself, which it does. |
| `spark.sql.sources.provider='orc'` — a Spark datasource table | `MSCK REPAIR TABLE` is unreliable; explicit `ALTER TABLE … ADD/DROP PARTITION` is the right instrument, which is what the executor uses. |
| `used_worfklow` is **misspelled in the DDL** | Preserve the spelling in any query. This engine does not read the column; if it ever does, it must not "fix" it. |
| `duration` is a formatted string (`0h 8mn 54s`) | Never parse it. Recompute from `end_date - creation_date`. |
| `user_launcher` = `sttengineihm` (service account), `real_user_id` = the person | The audit link for IHM-triggered runs, and the pair the catalogue now surfaces. |

### Settled: one partition level, `runId=<uuid>/`

The layout note raised a suspected `runId=<uuid>/runid=<uuid>/` nesting. **Confirmed there is one
level: `runId=<uuid>/`** — as Q10 answered. That is what the engine assumes throughout:

- a run expands to `<database location>/<table>/runId=<uuid>` (`EngineRun.partitionDirectory`);
- the purge unit is that directory, because the inventory stops at the leaf and a run partition holds
  files and no sub-directory;
- the run catalogue recovers the run id from that segment of each path.

**The one hazard left is the spelling, not the depth.** The engine writes `runId=` on disk and the
metastore stores `runid`, and the two reach the path helpers from different directions. A
case-sensitive key match would simply not recognise a partition as a run — no error, no row in the
catalogue, and a purge that quietly has nothing to do, which is this codebase's recurring failure
mode. `extractPartitionValue` and the selection SQL therefore match the partition KEY
case-insensitively while returning the value exactly as written: the key is a name, the value is an
identity.

---

## 14quater. No paths in the configuration

A database's location is written in exactly one place on Promethee — the Hive metastore. Any copy of
it in a purge configuration is a second answer to a question that already has one, and a copy that
will drift: the day a database is moved, the purge is still pointed at yesterday's directory, finding
nothing or finding something else. Both failures look like "there was nothing to purge".

So the engine asks the catalogue, the way `com.bnp.str.utilities.audit.RunAudit` already derives its
own table location:

```scala
val dbPath = spark.catalog.getDatabase(database).locationUri
```

The same argument applies one level up, to the database's NAME. The run configuration already
declares it in `output.database.name`, so the purge configuration does not name it either — it reads
it from the run. What that leaves:

| Setting | What it holds | Where it comes from |
|---|---|---|
| `engine.name` | which engine's conventions to read a run configuration with | the operator |
| `engine.runConfPaths` | the run(s) to purge | TWIST, which launched them |
| `scan.roots` | **empty** for an engine-driven run | defaults to the engine's database locations, from the metastore |
| `scan.allowedRoots` | the containment boundary | **written by hand, on purpose** |

This is also why the run **catalogue** takes a run configuration even though it lists runs rather
than acting on one: it has to know which database to look in, and the honest way to learn that is to
read it from a run of that database. TWIST holds every run's configuration, and `run_history.used_conf`
records the path of each — so it always has one to hand.

`allowedRoots` is the one path that stays. It is a safety limit rather than a lookup: it says what
this engine may never step outside of, and deriving it from the same catalogue it exists to constrain
would defeat the point of having it.

`engine.databaseLocation` survives only as a fallback for a session with no metastore to ask, and
using it logs a warning saying exactly what it is — a copy that will drift.

### Locally

A laptop starts with an empty in-memory metastore, so there is nothing to ask. Rather than let the
local configuration name a path and take a different code path from the cluster,
`purge_app.technical.localDatabases` registers the fixture database in that metastore
(`CREATE DATABASE … LOCATION`) and the resolution is then identical in both places. It refuses to run
anywhere but a local session: a purge engine that could create databases on Promethee is not what
anyone wants, and the guard is the environment rather than a flag someone could set by accident.

---

## 15. Open questions for the team

> **The live list is `docs/purge/OPEN_QUESTIONS_PURGE.xlsx`**, with an empty BUSINESS ANSWER column
> for the replies. The CSV beside it is the source of truth — it diffs cleanly in git — and the
> workbook is regenerated from it:
>
> ```
> python tools/open_questions/build_questions_workbook.py >     --csv docs/purge/OPEN_QUESTIONS_PURGE.csv >     --out docs/purge/OPEN_QUESTIONS_PURGE.xlsx >     --title "Purge engine - open questions"
> ```
>
> Feed each agreed answer back into the CSV's "Decision so far / answer" column and regenerate.
> The summary below is the same list in prose.

1. **Object vs row granularity.** v1 deletes paths and partitions. Is there a GDPR / client-level
   requirement to delete *rows* inside a kept partition? That changes the engine substantially
   (rewrite-in-place, compaction, or a `DELETE`-capable format such as Iceberg / Delta).
2. **Regulatory retention floors.** Which domains have a legally mandated minimum (BCE / ACPR
   reporting, IFRS9, RWA)? These must be encoded as `legalHold` policies before P4 ships.
3. **Dependency referential.** Does a lineage source already exist (Atlas, Navigator, a TWIST
   table)? `PC05` is only as good as its input; without one we can bootstrap it from `run_history`
   inputs, but coverage will be partial.
4. **Trash configuration.** What is `fs.trash.interval` on Promethee, and is Trash enabled for the
   service account? It sets the real restore window and the credibility of guarantee #8.
5. **Identity model.** Service account with wide delete ACLs, or `--proxy-user` per requester?
   Preference is `--proxy-user`; it needs the Kerberos / impersonation setup to allow it.
6. **Archive before hard delete.** Is there an archive tier (cold HDFS, tape, S3-compatible) that
   `PC07` should check, or does `HARD` simply mean gone?
7. **Managed vs external Hive tables.** Confirmed that all engine outputs are EXTERNAL with
   `external.table.purge = FALSE`? A managed table changes what `DROP PARTITION` does to the data.
8. **Which engine after projection and the classic simulator?** Each further engine needs one
   `EngineDescriptor` — the keys naming its run id, its outputs and its inputs — and nothing else;
   both granularities are now implemented end to end. Which one is next, and does any engine write
   output that is NOT declared in its run configuration?
9. **Double-nested run partitions.** The layout note reports a possible `runId=<uuid>/runid=<uuid>/`
   nesting under `term_structure` that the screenshot does not confirm. If it is real, the inventory
   treats the inner directory as the purge unit; worth verifying with `hdfs dfs -ls` before the first
   real perimeter.
10. **Jar packaging.** Same jar as `file_transform_engine` (one more module, reusing
   `com.bnp.str.utilities`), or a separate artifact with the utilities extracted into a shared
   library? Recommendation: same jar for v1, extract later if a second consumer appears.
