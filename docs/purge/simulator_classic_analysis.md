# Simulator (classic) — what a run is, and what purging one would have to remove

Source: the TWIST *SIMULATION* row for simulation **4363** and the run configuration it was launched
with, `localRun/purge/input/simulator_classic/conf.properties` (generated 07/09/2026 by `539693`,
jar `str_simulateur-bigData-2026_1.6.10-RELEASE`).

This is the analysis that has to be settled **before** a `SimulatorClassicEngine` descriptor is
written, because the simulator breaks four assumptions the projection MVP was built on. Each of them
fails in the same direction — a scope that matches nothing, a purge that reports success having
removed nothing — which is this codebase's recurring failure mode and the reason
`docs/purge/TECHNICAL_SPECIFICATION.md` §3bis insists nothing is inferred from a naming convention.

Throughout: **confirmed** = read directly from the configuration or the screen; **inferred** = the
most likely reading, needing a yes from the simulator team before code depends on it.

---

## 1. The run, as the two sources describe it

| | TWIST screen | Run configuration |
|---|---|---|
| Simulation id | `4363` | — (not in the file) |
| Simulation name | `SP26Q2_v24jun_v1_Scen26Q1_vu26Q2_v0` | — |
| Type / "Off." | Starting point | `run.simulation.category=STAPO` |
| Locked | `No` | — |
| Launched | `04/09/2026 18:00:17` | `20260904` inside the table names |
| User | (display name) | `run.realUserId=539693` |
| As-of date | `2026Q2` | `parameter.asOfDateQuarter=2026Q2` |
| Portfolio | `starting_point_ifrs9_n…` | `input.path.portfolio=…/starting_point_ifrs9_no_adjust_2026Q2_260624_v1/FACILITY` |
| Projection id | `10551` | header comment `Id Projection : 10551`, and `…/IHM/10551/…` in the inputs |
| Projection | `IFRS9_scn26Q1_vu26Q2_v0` | — |
| Parameter | `Input_STE_ifrs926Q2_SICR3_v2` | header comment `Id Parametre`, and the parameter input paths |
| Archive status | (column present) | — |
| Run id | — | `runId=5bbad7c8-8e7d-4e88-8106-f00ff4345e0b` |

**The two identities do not meet anywhere.** TWIST knows simulation `4363`; the configuration knows
run id `5bbad7c8-…`; and — see §3 — the tables on disk carry *neither*. Whatever binds them has to
come from `RUN_HISTORY` or `run_metadata`, not from a path.

### Keys, against the projection descriptor

| What | Projection | Simulator (classic) |
|---|---|---|
| run id | `projection.run.id` | `runId` (top-level, no prefix) |
| as-of quarter | `parameter.asOfDateQuarter` | `parameter.asOfDateQuarter` (same) |
| run name | `projection.name` | **none** — the name lives in TWIST only |
| run type | `projection.run.type` | `run.type=Normal`, plus `run.simulation.category=STAPO` |
| launched by | `projection.twist.user` | `run.realUserId` |
| launch type | — | `run.launch.type=TWIST` |
| database | `output.database.name` | `global.output.database.name=dbsimulateur` |
| output tables | `output.table.name.*` (~30) | `output.externalTable.name*` (see §3) |
| own history | `projection.history.table.name` (qualified) | `run.history.tablename=RUN_HISTORY` (**not** qualified) |
| inputs | `input.path.*` | `input.path.*` **and** `input.lgd_forward_looking.*.path` |

Two of those are traps rather than differences, and are picked up in §5.

---

## 2. Granularity: TABLE, and that is now a supported shape

A simulator run does not write `runId=<uuid>` into a set of standing tables. It **creates tables of
its own**, named after the simulation:

```
dbsimulateur.STAPO_sp_starting_point_ifrs9_no_adjust_2026Q2_260624_v1_py_10551_pa_Input_STE_ifrs926Q2_SICR3_v2_20260904_fac
dbsimulateur.STAPO_…_20260904_fac_measure_ByTermOutput
```

Read left to right, the name is the run's whole identity:

| Segment | Meaning | From |
|---|---|---|
| `STAPO` | simulation category | `run.simulation.category` |
| `sp_starting_point_ifrs9_no_adjust_2026Q2_260624_v1` | starting-point portfolio | the portfolio directory name |
| `py_10551` | projection id | `Id Projection` |
| `pa_Input_STE_ifrs926Q2_SICR3_v2` | parameter id | `Id Parametre` |
| `20260904` | launch date, `yyyyMMdd` | the run's launch date |
| `_fac`, `_fac_measure_ByTermOutput` | which output | the `output.externalTable.name*` key |

So this is **`granularity = TABLE`** — §16.Q1's exception, and the case the engine was just taught to
handle: a table-granular run expands to `(database, table, '')` and the table's own directory rather
than `<table>/runId=<uuid>` (§3bis). That part is already in place and unit-tested.

Two consequences fall straight out of the name above:

- **`runToken` is left at its default, and that is a known weakness.** The default for a
  table-granular engine returns the run id, and the run id appears nowhere in these table names, so
  PC04's path token cannot match. Overriding it would mean inventing a rule for turning a history row
  into a table name, which is exactly the naming-convention guessing this design forbids. It stays at
  the default until question 1 of §6 is answered; PC04 keeps catching an unfinished run through
  `end_date IS NULL` in the meantime.
- **The run catalogue cannot recover a run id from a path.** Its table-granular default takes the
  directory name as the run's identity, which here yields the *table* name. That is a usable identity
  for the scope screen (it is what a person recognises), but it will not join to `RUN_HISTORY` on
  `run_id`. §6 lists this as the first thing to settle.

---

## 3. Where the data actually is — the assumption that breaks

Projection tables live under the database location, so `<database location>/<table>` finds them and
`EngineRunReader` asks the metastore for the database and appends the table name.

**The simulator's do not.** Its outputs are EXTERNAL tables whose location is a production output
tree, entirely outside `dbsimulateur.db`:

```
output.path.run.directory                     = /Projects/STCreditRisk_STE/Simulateur_Production/Output_Simulateur/
output.path.exportdirFacilityOutput           = …/Output_Simulateur/facs_%s_%t/STAPO_…_20260904_%t
output.path.exportdirFacilityMeasurementByTermOutput = …/Output_Simulateur/facs_%s_%t/STAPO_…_fac_measure_ByTermOutput
output.externalTable.activeCreation           = true
```

Three separate problems live in those four lines.

**(a) The database location is the wrong root.** `<database location>/<table>` would build a path
under `dbsimulateur.db` that does not exist. For this engine the path must come from the **table's
own location in the metastore** (`DESCRIBE FORMATTED` / `spark.catalog`), not from the database's.
`EngineRunReader.databaseLocation` is therefore not enough, and §7 lists the change.

**(b) The paths carry unresolved placeholders.** `facs_%s_%t` and the trailing `_%t` are templates.
`%s` is **inferred** to be the scenario — `scenarios=Adverse,Central,Optimistic,Extreme,Secto`, five
of them, and the directory is *per scenario*. `%t` is **not identified** and must be, because a path
containing it is not a path: `PurgeGuard` and PC03 both refuse any target still carrying a wildcard,
so an unexpanded template would abort the run — which is the safe failure, but a failure.

**(c) `output.path.run.directory` is the root of every simulator run ever written.** It is a
configuration value that looks exactly like "where this run put its output" and is nothing of the
sort. Anything that collected it as this run's output directory — the way the projection descriptor
collects `cluster.output.directory` — would put `/Projects/STCreditRisk_STE/Simulateur_Production/Output_Simulateur/`
in a purge scope. `PurgeGuard`'s depth and denylist rules would probably not save it: it is deep
enough and not on the list. **This single key is the most dangerous line in the file** and the
descriptor must never treat it as an output.

---

## 4. Not everything a run touches is the run's

A simulator run writes into three different kinds of place, and only the first is purgeable.

| # | What | Keys | Purgeable? |
|---|---|---|---|
| 1 | Per-run external tables | `output.externalTable.nameFacilityOutput`, `…nameFacilityMeasurementByTermOutput` | **Yes** — this is the run |
| 2 | A shared, partitioned results table | `global.output.table.name=simulation_results_fac_partitioned_full` | **No, not as a table.** Shared across runs; at most this run's partitions |
| 3 | The run metadata / history | `global.output.table.metadata=run_metadata`, `run.history.tablename=RUN_HISTORY` | **Never** — it is the evidence the purge was legitimate |

Item 2 is the awkward one: a simulator run is table-granular for its own outputs **and**
partition-granular inside the global table. The engine's granularity is currently one value per
descriptor. Either the global table is out of scope for v1 (simplest, and probably right), or
`EngineDescriptor` grows a per-output granularity. **This needs a business answer before code.**

Item 3 has a direct parallel in projection, where `run_history` is excluded from the scope and named
as protected. `run_metadata` and `RUN_HISTORY` must be treated the same way.

### What must be protected as shared input (PC14)

Every `input.*` path in this file is read by many runs and owned by none:

- external referentials — `…/ReferentielExterne/external_referentials_newJar/3405/*`
- internal referentials — `…/ReferentielInterne/RefInternes2026Q1_v0_newJar_v0/3409/*`
- the parameter set — `…/Simulateur/Parameter/Input_STE_ifrs926Q2_SICR3_v2/3764/*`
- the portfolio and its exclusion file — `…/portfolio/starting_point_ifrs9_no_adjust_2026Q2_260624_v1/*`
- SICR III outputs — `…/STRESSTEST/SICRIII/Output/3123/*`
- **another engine's output** — `…/Projection_Production/Output_Cluster/IHM/10551/*`

That last group deserves its own note.

### The simulator consumes projection 10551's output

`input.path.prj.structure.terme`, `input.path.projection`, `input.lgd_forward_looking.*.path` and
others all point into `/Projects/STCreditRisk_STE/Projection_Production/Output_Cluster/IHM/10551/`.
Projection run 10551 is a *different engine's run*, and this simulation is a live consumer of it.

Purging that projection run would silently break the ability to re-run or explain this simulation.
That is precisely **PC05 `DOWNSTREAM_DEPENDENCY`**, which reports UNKNOWN today because no lineage
referential exists (§16.Q4). This file is a worked example of the lineage that referential would
hold, and an argument for bootstrapping it from run configurations: every simulator conf names the
projection it consumed, so the edge *projection 10551 → simulation 4363* is already written down.
PC14 does not cover it — PC14 protects the inputs of the runs **being purged**, and here the run
being purged would be the projection.

---

## 5. Hazards specific to this file

Ordered by how quietly each one fails.

**H1 — duplicate keys silently drop a protected input.** `input.path.projection` appears twice with
**different** values (`migration_matrix.csv`, then `z_results.csv`). `Properties.load`, which
`EngineRunReader.readProperties` uses, keeps the last and discards the first. A path that should have
been protected by PC14 would simply not be in the list, with no warning. (Here the lost file is also
named by `input.path.amortization_projection`, so it survives by luck — luck is not a mechanism.)
`input.path.edf.note.contrepartie` and `input.path.parameters.stressLgd` are also duplicated, though
with identical values. **Fix: read the file line by line, collect inputs as a multimap, and log every
key that appears twice with different values.**

**H2 — `output.path.run.directory` is a shared root, not this run's output.** See §3(c). It must be
explicitly excluded, and the exclusion needs a test that would fail loudly if someone later added a
generic "collect every `output.path.*`" rule.

**H3 — a declared table may never have been written.** The output switches decide what actually
exists:

```
output.enable.export.facility                   = true    -> the _fac table exists
output.enable.export.facility.measurement       = NONE    -> nameFacilityMeasurementOutput is EMPTY
output.enable.export.facility.measurement.term  = false   -> _fac_measure_ByTermOutput likely absent
output.monte.carlo.enable.export.*              = false   -> the monte-carlo tables were not written
```

Scoping a table that was never created produces a candidate that matches nothing — the silent
no-op again. The descriptor must read the enable flags, not just the name keys.

**H4 — template keys look like table names.** `output.externalTable.name=output_%s_%t` and the three
`output.monte.carlo.externalTable.name*` values are **patterns**, not names. Only the fully-resolved
`output.externalTable.name<Something>Output` keys may be read as tables, and even those must be
rejected when empty (`nameFacilityMeasurementOutput=`) or still containing `%`.

**H5 — two keys name the same table.** `nameFacilityOutput` and `nameFacilityWeightedOutput` hold the
identical value. Deduplicate, or the same table is purged twice and `purge_detail` double-counts the
bytes freed.

**H6 — the table names are database-qualified.** Unlike projection's bare `output.table.name.*`, these
values are `dbsimulateur.<table>`. The prefix must be split off, and checked against
`global.output.database.name` rather than assumed.

**H7 — the history table is not qualified.** `run.history.tablename=RUN_HISTORY`, upper case and with
no database. **Inferred** to be `dbsimulateur.RUN_HISTORY`; needs confirming, since PC04 reads it to
decide whether a job is still writing, and a history table that cannot be resolved makes PC04 report
UNKNOWN for every simulator purge.

**H8 — `_nosecto` variants. Confirmed on 2026-09-08: every output may exist twice.**
`output.suffix.for_non_secto_table=nosecto` names the suffix, and the four output keys name only the
full-perimeter table of each pair. A purge built from those keys alone removes half of what the run
wrote and leaves the twin behind — the "half-deleted run no consumer can interpret" of §3bis.
Each declared table is therefore scoped together with `<table>_<suffix>`, the suffix read from the
configuration rather than assumed, and a name already carrying it is not suffixed twice. A twin that
was never written costs nothing: it is in no metastore, resolves to no location, matches no
candidate, and never reaches the manifest — so scoping one that *might* exist is the safe direction.

**H9 — `Locked` is a business hold the engine cannot see.** The TWIST screen has a `Locked` column
(`No` for 4363). A locked simulation is presumably one nobody may remove — which is what PC02
`LEGAL_HOLD` is for — but the flag lives in TWIST, not in the configuration or the metastore. Either
TWIST refuses to submit a locked simulation, or it passes the flag through and PC02 reads it.
**Business decision, and it should be made explicitly rather than by omission.**

---

## 5bis. Settled on 2026-09-07: what a purge of this engine removes

The business named the scope: **the four `output.externalTable.name*Output` keys, and nothing else.**

```
output.externalTable.nameFacilityOutput                  = dbsimulateur.STAPO_...20260904_fac
output.externalTable.nameFacilityWeightedOutput          = dbsimulateur.STAPO_...20260904_fac   <- same table
output.externalTable.nameFacilityMeasurementByTermOutput = dbsimulateur.STAPO_..._fac_measure_ByTermOutput
output.externalTable.nameFacilityMeasurementOutput       =                                      <- empty
```

Two distinct tables, then. That answer closes more than it looks:

| Was open | Now |
|---|---|
| §4 item 2 — is the global `simulation_results_fac_partitioned_full` in scope? | **No.** It is not one of the four keys. One granularity per descriptor is enough, and `EngineDescriptor` needs no per-output one. |
| H4 — templates read as table names | **Closed by enumeration.** The four keys are listed explicitly in the descriptor; `output.externalTable.name` and the monte-carlo keys are never consulted. |
| H5 — the same table named twice | **Closed.** Deduplicated to one entry, so `purge_detail` cannot double-count the bytes freed. |
| H3 — a declared table that was never written | **Closed without reading the enable flags.** The empty value drops out on its own, and a table that turns out not to exist is `SKIPPED_ABSENT` by PC13 — which is what keeps replaying a manifest safe. |
| §3(b) — what is `%t`? | **No longer blocking.** Since no output *directory* is purged, no template has to be expanded. |
| §3(c) / H2 — `output.path.run.directory` | **Closed by construction.** The descriptor collects no output directory at all, and a test asserts it. |
| §3(a) — the data is outside the database location | **Handled.** The scope asks the metastore for each table's own registered location, which also resolves whatever `%s`/`%t` stood for without this engine having to know. |

What a purge removes is therefore: **the tables those keys resolve to — each together with its
`_nosecto` twin (H8) — and the data at their registered locations.** For simulation 4363 that is two
declared tables and up to two twins. Nothing that is only a path, nothing shared, nothing global.

---

## 6. Questions still open

1. **How does a table on disk get back to a run id?** The names carry portfolio, projection,
   parameter and date, but neither `runId` nor TWIST's simulation id. Does `RUN_HISTORY` or
   `run_metadata` carry the table name, or must the join be rebuilt from the name's parts? Two
   things wait on this: the run catalogue's ability to list simulator runs for the scope screen, and
   PC04, whose "is a job still writing here?" token cannot be built from a run id that appears in no
   path. Until it is answered, **PC04 is weak for this engine** — it still catches an unfinished run
   through `end_date IS NULL`, but cannot tie one to a table.
2. **Are `dbsimulateur.RUN_HISTORY` and `run_metadata` per-run or shared?** The descriptor qualifies
   the first as `dbsimulateur.run_history` (H7) and treats both as never purgeable; confirmation is
   still wanted.
3. **Do `Locked` and `Archive Status` gate a purge?** (H9) — and if so, does TWIST enforce them, or
   pass them to the engine so PC02 can?
4. **Is one simulation always one configuration file?** If a simulator run can be re-launched into
   the same tables, `output.append.method=OVERWRITE` says the second run replaces the first, and
   "which run does this table belong to" has more than one answer.

---

## 7. What building this will take

Beyond the descriptor itself, in rough order:

| # | Work | Why |
|---|---|---|
| 1 | `SimulatorClassicEngine extends EngineDescriptor`, `granularity = TABLE` | **done** — §2, §5bis |
| 2 | Duplicate-key detection when a run configuration is read | **done** — warns and names the keys (H1) |
| 3 | Table resolution from the **table's** metastore location, not the database's | **done** — §3(a) |
| 4 | Refuse any value still carrying a `%` template; enumerate the four output keys | **done** — H4, §5bis |
| 5 | Collect no output directory at all | **done**, with a test — H2, §3(c) |
| 6 | A `localRun` fixture: the tables on disk, and the metastore entries pointing at them | **to do** — needed before a simulation can be rehearsed end to end |
| 7 | `DROP TABLE` in `PurgeExecutor` — trash the data first, drop the metadata second — then lift `PurgeGuard`'s table-granularity refusal | **done** — §7.7 of the specification |

Items 1–5 and 7 are in. Item 6 — a local fixture — is what remains before a simulator purge can be
rehearsed end to end on a laptop.

### `external.table.purge = TRUE`, confirmed 2026-09-07

The business confirmed it for the projection and the simulator tables alike. That settles how the
deletion has to be ordered, and it is the opposite of reassuring: `DROP TABLE` on these tables
deletes the DATA, through Hive, outside this engine's deletion path and with no guarantee that Trash
is honoured. Dropping first and trashing after would destroy the 7-day restore window that is the
whole of what "reversible" means here.

So a table-granular object takes the same two steps a partition does, in the same order:

```
1. move the table's data to Trash    -> recoverable for fs.trash.interval (7 days)
2. DROP TABLE                        -> the location is already empty, so this is pure metadata
```

`execution.dropHiveTable` (default `true`) turns step 2 off for a perimeter that wants the data gone
and the definition kept. Two guards sit around it: the partition drop is attempted first, so the
larger instrument is only reached when the smaller one did not apply, and a row carrying a partition
spec is refused as a table drop even if it claims to be a whole table.

### What the engine does today, for this engine

A `SimulationDriver` run against a simulator configuration scopes the two tables, protects the
inputs, and produces a manifest and a report. `MainDriver` in an executing mode trashes each table's
data and drops the table, recording `TABLE_DROPPED` per object in `purge_detail`. What is missing is
only the local fixture that would let this be rehearsed off-cluster.
