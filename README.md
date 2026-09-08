# STR Purge Engine

A Spark engine that **removes** data from the Datalake on Promethee — and, more to the point, that can
prove afterwards what it removed, why it was allowed to, and who asked.

Today the Datalake is only ever written to. `addons`, `ageing`, `tseadfwd`, `projection` and the rest
read, transform and write, run after run, quarter after quarter; nothing takes the obsolete vintages
away. Storage grows, small-file pressure grows, and there is no traceable answer to *"who deleted
what, when, and was it allowed?"*. This engine closes that loop:

1. **Inventory** what exists in a declared scope (HDFS paths and Hive tables/partitions);
2. **Select** the candidates — from an engine run, an explicit selection, or a retention policy;
3. **Control** every candidate against fifteen business and technical rules, and render a report a
   human can sign off;
4. **Execute** the deletion of the approved candidates only, recoverably;
5. **Audit** all of it, at run level and at object level, in immutable ORC.

It is packaged, configured and launched exactly like the other STR engines: TWIST generates a HOCON
`application.conf` and runs `spark-submit`. No REST server, no new protocol, no state held in the
engine.

---

## The two ideas the rest follows from

**Never delete in one shot.** A purge is always two phases:

```
SIMULATE  ->  manifest + control report  ->  a human looks  ->  EXECUTE (manifest only)
```

The execution phase never re-derives what to delete. It replays the manifest a simulation produced,
re-verifies it, and deletes strictly what is listed. Anything that drifted in between is skipped and
reported — see `PC12`.

**The unit of a purge is a RUN, not a date.** Most engines on Promethee partition their output by
run: one projection run writes `runId=<uuid>` into thirty-odd tables at once, and those partitions
are one thing — produced together, meaningful together, purged together or not at all. So a request
names an engine and one or more of its runs, and what that run produced is read from **the engine's
own `conf.properties`**, the file it was launched with, which TWIST already keeps. Nothing is guessed
from a naming convention: the run id, the tables, the database, the output folders and the shared
inputs all come out of that file.

That also means **no paths in the purge configuration**. A database's location lives in the Hive
metastore, so the engine asks the metastore; the database's *name* lives in the run configuration, so
the engine reads it there. The single hand-written path is `scan.allowedRoots` — a containment
boundary, not a lookup, and deriving it from the catalogue it exists to constrain would defeat it.

---

## Entry points

Four drivers, each submitted the same way:

```
spark-submit --class <driver> target/str-purge-engine.jar <path/to/application.conf>
```

| Driver | Does | Deletes? |
|---|---|---|
| `job.RunCatalogDriver` | Lists the runs of an engine still on disk, with size and history status. Fills TWIST's scope screen. | **No** |
| `job.SimulationDriver` | Inventory → selection → controls → fingerprinted manifest + HTML report. TWIST's "Analyse". | **No** |
| `job.InventoryDriver` | The raw scan of a scope, and optionally the metastore's view of it. | **No** |
| `job.MainDriver` | The above, plus — behind `request.mode` — the deletion. | **Yes** |

The three read-only drivers hold that property *permanently*: they contain no destructive code at
all, rather than being a safe mode of a class that can also delete. A mistake in the conf, in the mode
flag or in the IHM therefore cannot turn an analysis into a deletion.

`MainDriver` reads `request.mode`:

| Mode | Behaviour |
|---|---|
| `SIMULATE` (default) | Builds the manifest and the report. Deletes nothing. |
| `EXECUTE` | Replays the manifest of `request.manifestRunId`. Builds nothing. |
| `SIMULATE_AND_EXECUTE` | Both in one submission, with nobody looking in between. Says so loudly in the log. |

A mistyped mode is refused rather than interpreted — neither silently EXECUTE nor silently SIMULATE.

---

## Quick start — the local rehearsal

`localRun/purge/` carries a miniature Datalake (a `dbprojection.db` with two projection runs across
eight tables, a run history, shared model inputs) and an `application.conf` wired to it. Everything is
relative to the project root, so run from there.

**What you need:** a JDK (the pom targets Java 8; 11 works and is what this was last built with),
Maven 3.9, and a local Spark 3.5 on `PATH` with `SPARK_HOME` set — Spark and Hadoop are `provided`,
so the fat jar does not carry them. On Windows you also need `HADOOP_HOME` pointing at a directory
whose `bin/` holds `winutils.exe` and `hadoop.dll`, or the filesystem calls fail before the engine
starts.

```bash
mvn -q clean package -DskipTests

# 1. which runs are on disk?
spark-submit --class com.bnp.str.purge.job.RunCatalogDriver \
             target/str-purge-engine.jar localRun/purge/application.conf

# 2. what would a purge of one of them select, and what do the controls say?
spark-submit --class com.bnp.str.purge.job.SimulationDriver \
             target/str-purge-engine.jar localRun/purge/application.conf
```

Step 2 writes an ORC manifest under `localRun/purge/output/purge_manifest/` and the HTML report at
`localRun/purge/reports/PRG-LOCAL-0001.html`. Open the report: the banner at the top lists the
controls that could **not** be evaluated, above the counters, because an approver reading a green
report has to know which greens were actually checked.

To watch the engine actually delete, set `request.mode = "SIMULATE_AND_EXECUTE"` and submit
`MainDriver`. It moves the fixture partitions to Trash and writes one `purge_detail` row per object.
`localRun/purge/input/` is version-controlled, so `git checkout` puts the fixture back; everything the
engine writes (`output/`, `reports/`, `run_history/`) is git-ignored.

### The simulator rehearsal

`localRun/purge/application_simulator.conf` drives the same thing against the **classic simulator** —
the table-granular engine, where a run IS its tables:

```bash
spark-submit --class com.bnp.str.purge.job.SimulationDriver \
             target/str-purge-engine.jar localRun/purge/application_simulator.conf
```

Its fixture reproduces the shape of the cluster rather than a convenient version of it: tables that
live nowhere near their database, the engine's own `run_history` and shared results table sitting
inside the scope root, and a neighbouring simulation the purge must leave alone. The run
configuration is the real one TWIST generated, used unedited — nothing in it is rewritten for local
use, because nothing in it names a path the engine relies on.

To watch it delete, set `request.mode = "SIMULATE_AND_EXECUTE"` and **give the run a Trash window**:

```bash
spark-submit --conf spark.hadoop.fs.trash.interval=10080 \
             --class com.bnp.str.purge.job.MainDriver \
             target/str-purge-engine.jar localRun/purge/application_simulator.conf
# -> PURGE SUCCESS - 3 object(s) removed, 228 B freed [TABLE_DROPPED=3]
```

A laptop has `fs.trash.interval = 0`, and the engine **fails** an object rather than falling back to
an unrecoverable delete — so without that flag every object fails and the run ends `PARTIAL`. That
is the design working, not a fixture problem. Restore the tree afterwards with
`git checkout localRun/purge/input/simulator_classic`.

### Knobs worth turning

Two in `localRun/purge/application.conf`, because they show the engine refusing rather than working:

- **`request.asOfDate`** is pinned to `2027-01-15`. The fixture run is an as-of 2026Q3 projection and
  the policy grants one quarter of retention, so it is just old enough to go. Set the date before
  `2026-12-30` and `PC01` blocks the whole run — the engine declining to delete data still inside its
  retention.
- **`purge_policy.rules[0].keepMinVersions`** is `0`. Set it to `1` and `PC06` refuses to remove the
  most recent run of a table however the scope was drawn, which is what a production perimeter wants.

A laptop starts with an empty in-memory metastore, so `purge_app.technical.localDatabases` registers
the fixture database in it (`CREATE DATABASE … LOCATION`). That keeps the resolution path identical to
the cluster's — the engine always asks the catalogue — and it refuses to run anywhere but a local
session, because a purge engine that could create databases on Promethee is not what anyone wants.

### The alarming lines that are not failures

A local run prints several things that read like errors and are not. Worth knowing before you go
hunting:

| Line | What it is |
|---|---|
| `ObjectStore: Failed to get database … NoSuchObjectException` | Hive populating an empty in-memory metastore. Normal on a laptop. |
| `EngineRunReader: Could not read 'dbprojection.run_history' … TABLE_OR_VIEW_NOT_FOUND` | The fixture has the run history on disk but never registers it as a table, so `PC04` reports **UNKNOWN** rather than PASS — which is the behaviour that section is about, visible in miniature. |
| `ERROR ShutdownHookManager: Exception while deleting Spark temp dir` | Windows holding the jar open after the JVM is done. It fires *after* the driver's own `End purge_engine` line; the run succeeded. |
| `derby.log` and `spark-warehouse/` appearing in the project root | Derby and Spark's local defaults. `derby.log` is git-ignored; `spark-warehouse/` is not, so don't commit it. |

The line that actually tells you what happened is the `PurgeControlMapper` summary:
`CONTROLS - CLEAR: 8 candidate(s) -> 8 deletable (0 with warning), 0 blocked; … 2 control(s) could not
be checked: PC04, PC05`.

---

## Configuration

One HOCON root block, `purge_app`, in the conventions of the other engines (`enable` flags on
outputs, an `audit { }` block). `localRun/purge/application.conf` is the annotated reference;
§10 of the technical specification is the complete one.

| Block | What it says |
|---|---|
| `request` | id, `mode`, requester and their LDAP groups, `manifestRunId`, and the `asOfDate` every retention is measured against |
| `engine` | `name` (which engine's conventions to read a run configuration with) and `runConfPaths` (the run(s) to purge) |
| `scan` | `allowedRoots` — the containment boundary, hand-written — plus roots, databases, depth, parallelism |
| `purge_policy` | the retention referential, inline or from a Hive table |
| `purge_scope` | an optional hand-picked selection on top of the engine scope |
| `controls` | the fifteen rules, each switchable, and the HTML report path |
| `guard` | the ceilings `PurgeGuard` asserts: bytes, objects, % of table, manifest age |
| `execution` | strategy, whether to drop the Hive partition, error handling |
| `purge_run_catalog` / `purge_inventory` / `purge_catalog` / `purge_manifest` / `purge_detail` | the outputs |
| `audit` | the shared `run_history`, under `module_name = purge` |

`asOfDate` is pinned on purpose: a simulation and the execution that replays it must judge the same
objects against the same date.

Two smaller mechanisms live in that block and are easy to miss. `asOfDate` aside, the selection SQL
itself is overridable: set `purge_manifest.queryName` and the mapper loads that query from the HOCON
file at `sql_queries.path` instead of the built-in `PrimaryView.get_purge_candidates`, so a perimeter
with an unusual selection expresses it in configuration rather than forking the engine. It is empty in
the local conf — and `localRun/purge/sql/queries.sql.conf` does not exist, which costs nothing while
`queryName` stays empty and is the first thing to create if you set it.

---

## What a run produces

| Output | Written by | Contents |
|---|---|---|
| `purge_run_catalog` | `RunCatalogDriver` | one row per run of the engine on disk: size, file and table counts, `known_to_history`, `unfinished`, and `used_conf` — the run's own configuration path, so the scope screen can hand it straight to the simulation |
| `purge_inventory` / `purge_catalog` | `InventoryDriver` | the filesystem scan of a scope, and the metastore's view of the same scope |
| `purge_manifest` | `SimulationDriver`, `MainDriver` | ORC partitioned by `(purge_date, run_id)`: every candidate with its size, age, business date **and where that date came from**, policy, strategy, `decision` and the controls it failed |
| the HTML report | `SimulationDriver`, `MainDriver` | the control matrix, the counters, the offending objects per rule — what an approver actually reads |
| `purge_detail` | `MainDriver` in execute mode | one row per object touched, ORC partitioned by `(purge_date, run_id)`: status, bytes freed, `trash_path`, `restore_deadline`, error, and `source_engine` / `source_run_id` — the engine run the object belonged to |
| `run_history` | every driver that audits | the run history, `module_name = purge`, carrying the purged run ids in `scenarios` |

`purge_detail` and `run_history` live in **`dbpurge`**, the purge engine's own database — never in
the database being purged. An audit that sits inside its subject is an audit a wide enough purge can
take with it; `dbpurge` is no engine's output, so nothing an engine-driven scope expands to can
reach it. Both keep `external.table.purge = FALSE`, so even `DROP TABLE` leaves the ORC behind.

**Following a purge afterwards** — the three ids that answer "what did I remove?":

```sql
-- by the purge run
SELECT * FROM dbpurge.purge_detail WHERE run_id = '<purge run id>';
-- by the TWIST request, across every attempt of it
SELECT * FROM dbpurge.purge_detail WHERE request_id = 'PRG-2026-000142';
-- by the ENGINE run that was purged, whatever the engine's granularity
SELECT * FROM dbpurge.purge_detail WHERE source_run_id = '5bbad7c8-...';
-- what is still restorable, and until when
SELECT path, trash_path, restore_deadline FROM dbpurge.purge_detail
WHERE status IN ('TRASHED','PARTITION_DROPPED','TABLE_DROPPED')
  AND restore_deadline > current_timestamp();
```

The manifest carries a **fingerprint** — a SHA-256 over the sorted `(path, size_bytes,
modification_time)` triples — which the execution re-checks before touching anything.

`purge_detail` is a complete account of the manifest, not a list of successes: `TRASHED`, `DELETED`,
`PARTITION_DROPPED`, `TABLE_DROPPED`, `LOGICALLY_DELETED`, `SKIPPED_BLOCKED`, `SKIPPED_ABSENT`,
`SKIPPED_DRIFT`, `SKIPPED_ABORTED`, `FAILED`. That is what makes replaying a manifest safe, and replaying is how a
`PARTIAL` run is finished — everything already removed comes back `SKIPPED_ABSENT`.

---

## The controls

Every candidate goes through every enabled rule. A **BLOCKING** rule that fires sets the candidate to
`BLOCKED` and no approval overrides it; a **WARNING** only annotates.

| Id | Rule | Type | Fires when |
|---|---|---|---|
| PC01 | `RETENTION_NOT_REACHED` | BLOCKING | a policy matched and its retention is not reached |
| PC02 | `LEGAL_HOLD` | BLOCKING | a legal or regulatory hold covers the object |
| PC03 | `PATH_OUT_OF_SCOPE` | BLOCKING | outside `allowedRoots`, too shallow, denylisted, or escaping via `..`/symlink |
| PC04 | `ACTIVE_RUN` | BLOCKING | a run that touches this path has not finished, or finished less than `minQuietMinutes` ago |
| PC05 | `DOWNSTREAM_DEPENDENCY` | BLOCKING | a declared consumer feeds off it |
| PC06 | `MIN_VERSIONS_KEPT` | BLOCKING | the deletion would leave fewer than `keepMinVersions` vintages |
| PC07 | `NO_ARCHIVE` | BLOCKING | a `HARD` delete is asked for and no archive copy exists |
| PC08 | `NOT_AUTHORIZED` | BLOCKING | the requester does not hold the perimeter's purge group |
| PC09 | `BLAST_RADIUS` | BLOCKING | the run exceeds `guard.maxBytes` / `maxObjects` / `maxPercentOfTable` |
| PC10 | `RECENTLY_ACCESSED` | WARNING | HDFS `atime` within `recentAccessDays` |
| PC11 | `HIVE_METADATA_ORPHAN` | WARNING | the path is a registered partition; the executor must drop it too |
| PC12 | `MANIFEST_DRIFT` | BLOCKING (execute) | size or mtime no longer matches the manifest |
| PC13 | `ALREADY_ABSENT` | WARNING | the path is already gone |
| PC14 | `SHARED_INPUT` | BLOCKING | the object is an *input* of the runs being purged, or their run history |
| PC15 | `NO_RETENTION_POLICY` | WARNING | nothing but the explicit selection protects this object |

`PC03`, `PC09` and `PC12` cannot be switched off: `PurgeGuard` re-checks them in code, so a report
that omitted them would describe a run stricter than what it says.

**A control that could not run is never reported as PASS.** The report distinguishes `PASS`
(evaluated, nothing found), `SKIPPED` (switched off), `NOT EVALUATED` (belongs to the execution phase)
and `UNKNOWN` (the data it needed was missing — an empty `run_history`, no dependency referential, no
requester groups). "We checked and found nothing" and "we could not check" lead to very different
decisions, so they never share a colour.

---

## Safety

| Guarantee | Enforced by |
|---|---|
| Nothing deleted without a prior simulation | `mode` + `manifestRunId` required in `EXECUTE` |
| Only what is in the manifest is deleted | `PurgeExecutor` replays the manifest, never re-scans for targets |
| The manifest cannot change under you | fingerprint + per-object drift check (`PC12`) |
| Nothing deleted outside the declared roots | `scan.allowedRoots` (`PC03`) **and** `PurgeGuard` |
| No catastrophic path | `MIN_PATH_DEPTH = 4` and a denylist, hard-coded and non-configurable |
| No deletion under a live writer | `PC04`, keyed off `end_date IS NULL` in the engine's own history |
| Bounded blast radius | `PC09` + `guard.maxBytes` / `maxObjects` |
| Recoverable by default | `TRASH`, for the cluster's 7-day `fs.trash.interval` |
| Fully auditable | `run_history` + `purge_detail` + the HTML report, immutable ORC |
| Idempotent | an absent path is `SKIPPED_ABSENT`; replaying a manifest is safe |

Three implementation choices inside that are not obvious:

- **Trash first, drop second.** Every STR output is EXTERNAL with `external.table.purge = TRUE` —
  confirmed for projection and the classic simulator alike — so a bare `DROP PARTITION` or
  `DROP TABLE` deletes the data *through Hive*, outside this engine's deletion path and with no
  certainty that it honours Trash. So the executor moves the data to Trash, then drops a partition
  (or, for a table-granular run, a table) whose location is already empty: pure metadata, metastore
  consistent, 7-day window intact.
- **Trash unavailable is a failure, not a fallback.** `moveToAppropriateTrash` returns false when
  `fs.trash.interval` is 0 and leaves the data where it is. Reporting that as success would claim a
  purge that did not happen; falling back to a hard delete would destroy the recoverability the
  strategy was chosen for. The object fails, and the run ends `PARTIAL`.
- **Drift is measured the way the inventory measured.** A leaf directory's size is the sum of its
  files and its mtime the newest among them — compare against the directory's own `getLen` instead and
  nearly every partition looks changed, the objects are skipped, and the run reports SUCCESS having
  quietly purged nothing. That was a real bug, caught by the first end-to-end execution.

`PurgeExecutor` iterates on the driver, not the executors: deletion is a NameNode metadata operation,
fanning it out buys nothing and makes failure handling opaque.

---

## Project layout

```
src/main/scala/com.bnp.str.purge/
├── job/            InventoryDriver · SimulationDriver · RunCatalogDriver · MainDriver
├── common/         RunnerProvider · PrimaryRunner · MapperProvider · PurgeOutcome
├── engine/         EngineRun · EngineDescriptor (+ registry) · ProjectionEngine
│                   SimulatorClassicEngine
├── reader/         PrimaryReader · InventoryReader · CatalogReader · EngineRunReader
│                   PolicyReader · RunCatalogReader · ManifestReader
├── mapping/        PrimaryMapper · PrimaryView (the selection SQL) · ManifestView (fingerprint)
├── control/        CheckModel · CheckConfig · PurgeControlMapper · CheckHtmlView · CheckWriter
├── purge/          PurgeStrategy · PurgeExecutor · PurgeGuard      <- the only destructive package
├── writer/         PrimaryWriter · PurgeDetailWriter
├── audit/          PurgeAudit · PurgeDetailStore
├── sessionmanager/ StrSparkSessionManager · LocalDatabaseRegistrar
└── utility/        PrimaryConstants · PrimaryUtilities · DateUtils
```

Same layout, file names and class-name logic as `file_transform_engine`
(`com.bnp.str.addons`), with `control/` mirroring `tseadfwd/coherence` and `audit/` mirroring
`tseadfwd/audit`. The shared `com.bnp.str.utilities` package (`SparkConfLogger`, `audit/RunAudit`,
`RunAuditRecord`, `RunAuditStore`) is reused as-is.

**Adding an engine** is one `EngineDescriptor`: the keys naming its run id, its outputs, its inputs,
its history table, and its `granularity` (`PARTITION` or `TABLE`). Nothing else changes. `projection`
is the MVP and `simulator_classic` the first table-granular engine — where a run IS its tables rather
than a partition of each. Getting granularity wrong in either direction — dropping a table where a
partition was meant, or the reverse — is severe, so it is declared, never inferred, and the two
expansions are pinned against each other by `EngineGranularitySpec`.

Build: Scala 2.12.18 / Spark 3.5.4 / Hadoop 3.3.4, Java 8 target, fat jar `target/str-purge-engine.jar`.
Spark and Hadoop are `provided`. The pom's default `mainClass` is the read-only `InventoryDriver`, but
every submission names `--class` explicitly.

---

## Tests

```bash
mvn test
```

ScalaTest through `scalatest-maven-plugin` (Surefire is deliberately skipped — it finds no ScalaTest
suite on its own). Fourteen specs on a shared local Spark session (`SparkTestSession`):

`PrimaryUtilitiesSpec` · `DateUtilsSpec` · `PrimaryViewSpec` · `ManifestViewSpec` ·
`PolicyReaderSpec` · `InventoryReaderSpec` · `CatalogReaderSpec` · `EngineRunReaderSpec` ·
`RunCatalogReaderSpec` · `ProjectionEngineSpec` · `PurgeControlMapperSpec` · `PurgeGuardSpec` ·
`PurgeExecutorSpec` · `CheckHtmlViewSpec`

`PurgeExecutorSpec` runs against a temp filesystem: Trash moves and keeps the file, an absent path is
`SKIPPED_ABSENT`, one failure does not abort the run.

---

## Documentation

| File | What it is |
|---|---|
| [`docs/purge/TECHNICAL_SPECIFICATION.md`](docs/purge/TECHNICAL_SPECIFICATION.md) | the design: architecture, class-by-class, data model, the full configuration reference (§10), the TWIST screens (§11), the delivery phases (§14) |
| [`docs/purge/run_history_row_detail.md`](docs/purge/run_history_row_detail.md) | what `dbprojection.run_history` actually contains, captured from Hue — and the several defensive readings it settled |
| [`docs/purge/hdfs_dbprojection_layout.md`](docs/purge/hdfs_dbprojection_layout.md) | the on-disk layout of `dbprojection.db`, transcribed from the file browser |
| [`docs/purge/OPEN_QUESTIONS_PURGE.csv`](docs/purge/OPEN_QUESTIONS_PURGE.csv) | the open questions, source of truth; the `.xlsx` beside it is generated from it |

Regenerate the workbook after editing the CSV:

```bash
python tools/open_questions/build_questions_workbook.py \
    --csv docs/purge/OPEN_QUESTIONS_PURGE.csv \
    --out docs/purge/OPEN_QUESTIONS_PURGE.xlsx \
    --title "Purge engine - open questions"
```

---

## Not in v1

Deliberate omissions, so nobody goes looking for them in the code:

- **No restore.** `RestoreDriver` and TWIST's S6 are P6 design; nothing in `src/main` references it
  yet. Recovery today is HDFS Trash by hand, from the `trash_path` and before the `restore_deadline`
  that `purge_detail` records for every object.
- **No row-level deletion.** v1 deletes at object granularity — a path or a partition. GDPR-style
  "remove these records from a partition we keep" needs rewrite-in-place or a `DELETE`-capable format
  (Iceberg, Delta) and is a v2 topic.
- **Object stores other than HDFS are out of scope** — Kudu, HBase, Solr, Impala-managed storage.
- **No cross-cluster propagation.** A deletion here does not reach the DR cluster.
- **No scheduling.** Every run is submitted on demand by TWIST; a scheduled mode comes later.
- **Two engines.** `projection` and `simulator_classic` are the `EngineDescriptor`s in the registry
  — one of each granularity. Another engine is a descriptor, but it is not written yet, and each new
  one needs its own confirmation that everything it writes is declared in its run configuration.
- **`PC05` and `PC07` are implemented and inert.** There is no lineage referential on Promethee yet
  (so `PC05` reports UNKNOWN until one exists) and archive is on hold and may end up outside
  Cloudera's reach. Both stay in the report rather than being deleted from it, because a control that
  silently vanished is worse than one that says it could not check.

---

## Status

P1 → P5 (engine side) are implemented: skeleton, inventory, catalog, engine-aware run scoping,
retention policies, candidate selection, fingerprinted manifest, the fifteen controls, the HTML
report, and — since P4 — the execution itself. **The engine deletes.** P5 added the run catalogue the
IHM's scope screen needs; the TWIST screens are the IHM team's work. P6 — `RestoreDriver`, the restore
screen, policy referential CRUD, a scheduled mode — is design.

The twelve open questions were answered by the business on **2026-08-29**, and two of the answers
changed the model: **there are no automatic retention policies** (Q3) and **there is no four-eyes
approval step** (Q11). The business owns its data and holds the purge role. So what protects data is
no longer "a rule allowed it" but **"someone named it"** — and naming is exactly what an engine run
scope is. Two-phase simulation, manifest-only execution, containment and reversibility all survive
that change; default-deny-by-policy is replaced by default-deny-by-scope, and `PC15` keeps the
information `PC01` used to carry visible in the report. `guard.requireApproval` re-enables four-eyes
in `PurgeGuard`: implemented and inert, so a sensitive perimeter turns it back on with a configuration
change rather than a code change. §16 of the specification records the rest.
