# `dbprojection.run_history` — row detail + observed schema

Source: Hue SQL editor, "Détail de la ligne" modal.
`https://hue2.cdp.promethee.group.echonet/hue/editor?editor=126140`
Companion to `hdfs_dbprojection_layout.md` (same cluster, same database).

---

## 1. Sample row (verbatim)

| Column | Value |
|---|---|
| `run_history.run_id` | `dbddd6d8-ed80-4bac-9b59-505840f2e643` |
| `run_history.application_id` | `application_1767175294768_50884` |
| `run_history.used_jar` | `str_projection_engine-7.4-RELEASE.jar` |
| `run_history.used_conf` | `/Projects/STCreditRisk_STE/Projection_Production/Projection_Conf_Files/IFRS9_scn25Q4_scenDef_v0_secto_v4_Promethee_LGDtest_2026-02-03_15_51_20_9/conf.properties` |
| `run_history.used_worfklow` | `/Projects/STCreditRisk_STE/Projection_Production/Projection_Conf_Files/IFRS9_scn25Q4_scenDef_v0_secto_v4_Promethee_LGDtest_2026-02-03_15_51_20_9/workflow-projection.xml` |
| `run_history.user_launcher` | `sttengineihm` |
| `run_history.creation_date` | `2026-02-03 15:51:44.347` |
| `run_history.end_date` | `2026-02-03 16:00:38.379` |
| `run_history.duration` | `0h 8mn 54s` |
| `run_history.motor` | `projection` |
| `run_history.launch_type` | `TWIST` |
| `run_history.run_type` | `IFRS9` |
| `run_history.real_user_id` | `539693` |
| `run_history.status` | `succeeded` |

---

## 2. Authoritative DDL (`SHOW CREATE TABLE`)

Captured from `createtab_stmt` output. Verbatim, with line numbers from the source pane.

```sql
CREATE EXTERNAL TABLE `dbprojection`.`run_history`(
  `run_id` string,
  `application_id` string,
  `used_jar` string,
  `used_conf` string,
  `used_worfklow` string,
  `user_launcher` string,
  `creation_date` timestamp,
  `end_date` timestamp,
  `duration` string,
  `motor` string,
  `launch_type` string,
  `run_type` string,
  `real_user_id` string,
  `status` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
WITH SERDEPROPERTIES (
  'compression'='ZLIB',
  'delimiter'='\;',
  'header'='false',
  'path'='hdfs://hahdfsnameservice/Projects/STCreditRisk_STE/hive/databases/dbprojection.d…')  -- [truncated]
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://hahdfsnameservice/Projects/STCreditRisk_STE/hive/databases/dbprojection.db/run_hi…'  -- [truncated]
TBLPROPERTIES (
  'TRANSLATED_TO_EXTERNAL'='TRUE',
  'bucketing_version'='2',
  'external.table.purge'='TRUE',
  'spark.sql.create.version'='3.3.2.3.4.7190.0-382',
  'spark.sql.sources.provider'='orc',
  'spark.sql.sources.schema'='{"type":"struct","fields":[{"name":"run_id","type":"string","n…',  -- [truncated]
  'transient_lastDdlTime'='1787838834')
```

Lines 23, 29 and 36 run off the right edge of the editor; the truncation points are marked.
Re-capture with word wrap on if the full `spark.sql.sources.schema` JSON is needed.

### What the DDL settles

- **`used_worfklow` is confirmed misspelled at the DDL level**, not just in Hue's display.
- `creation_date` / `end_date` are real `timestamp` columns (my earlier guess of string was wrong).
  `duration` really is a `string`, and `real_user_id` really is a `string`, not numeric.
- **`run_history` is not partitioned at all** — no `PARTITIONED BY` clause. The `runId=` partition
  problem is confined to the result tables (`term_structure`, `term_structure_detailed`, …).
- No bucketing, no constraints, no `NOT NULL` — nothing enforces `run_id` uniqueness, which is
  consistent with the suspected duplicate in section 3.

### Things in the DDL worth acting on

- **`TRANSLATED_TO_EXTERNAL='TRUE'` + `external.table.purge='TRUE'`.** This table was created as
  *managed* by Spark and auto-translated to EXTERNAL by CDP Hive 3's metastore translation layer.
  With `external.table.purge=TRUE`, a `DROP TABLE` **deletes the underlying HDFS data** — it does
  not behave like a normal external table. This is directly relevant to the earlier
  `chr_detailed_2021_recette` recovery: tables in this database that look external are
  translated-managed and are not drop-safe.
- **`spark.sql.sources.provider='orc'` makes this a Spark datasource table, not a Hive-native
  table.** Spark reads the schema from `spark.sql.sources.schema` in TBLPROPERTIES, *not* from the
  Hive column list. Consequences: adding a column via `ALTER TABLE … ADD COLUMNS` updates the Hive
  columns but leaves Spark's view stale, and for the partitioned sibling tables this is also why
  `MSCK REPAIR TABLE` is unreliable — Spark datasource tables track partitions through the
  metastore's partition entries, so explicit
  `ALTER TABLE … ADD IF NOT EXISTS PARTITION … LOCATION` remains the right fix.
- **A `path` SERDEPROPERTY duplicating `LOCATION`.** That is the signature of a writer calling
  `.option("path", …)` explicitly. Combined with a `.partitionBy(…)` on an already-qualified
  path, this is exactly the pattern that produced the `runId=<uuid>/runid=<uuid>/` nesting in the
  result tables. Worth grepping the writers for `option("path"` alongside `partitionBy`.
- **`'delimiter'='\;'` and `'header'='false'` on an ORC table are inert leftovers** — CSV options
  passed through indiscriminately by the writer. Harmless, but they indicate a shared/copy-pasted
  write helper that doesn't branch on format.
- **`spark.sql.create.version='3.3.2.3.4.7190.0-382'`** — created by the CDP 3.3.2 build, i.e.
  pre-upgrade. Tables created after the 3.5.4 rollout will carry a different value; this property
  is a cheap way to inventory which tables predate the migration.
- **`transient_lastDdlTime='1787838834'`** → 2026-08-27 ≈ 13:53 UTC (15:53 Paris), which matches
  the `run_history` directory mtime of *August 27, 2026 03:53 PM* in `hdfs_dbprojection_layout.md`.
  The two captures are consistent.

### Notable design facts

- **`used_worfklow` is misspelled in the actual column name** (`worfklow`, not `workflow`).
  Preserve this exactly in any query or writer code — do not "fix" it silently.
- `duration` is stored as a **formatted string** (`'0h 8mn 54s'`), not seconds/millis, while
  `creation_date` and `end_date` are proper timestamps. Any aggregation (avg runtime, SLA
  reporting) should recompute from
  `unix_timestamp(end_date) - unix_timestamp(creation_date)` rather than parse `duration`.
  Consistency check on this row: 15:51:44.347 → 16:00:38.379 = 8 min 54.032 s ✓.
- `used_conf` and `used_worfklow` share the same parent directory, whose name encodes
  run_type + scenario + timestamp:
  `IFRS9_scn25Q4_scenDef_v0_secto_v4_Promethee_LGDtest_2026-02-03_15_51_20_9`
  → `<run_type>_<scenario>_<scenDef>_<secto>_<env>_<label>_<yyyy-MM-dd_HH_mm_ss>_<seq?>`.
  Trailing `_9` is unexplained — possibly a sequence/retry counter.
- The `application_id` cluster timestamp `1767175294768` = 2025-12-31 (UTC-ish), i.e. the
  YARN RM start time; all rows in the visible grid share it, so no RM restart across these runs.
- `user_launcher` (`sttengineihm`) matches the HDFS owner of engine-written directories in
  `dbprojection.db`. `real_user_id` is the separate human attribution field — this is the
  auditability link for the IHM-triggered runs.

---

## 3. Background grid rows (partially visible behind the modal)

| # | run_id | application_id | used_jar |
|---|---|---|---|
| 4 | `c2b31c29-c1aa-4584-9ade-1d9fdfdc58f3` | `application_1767175294768_56905` | `str_projection_engine-7.4-RELEASE.jar` |
| 5 | `1f5ec5c2-3192-422c-8d1c-3d4c32c3ede5` | `application_1767175294768_60791` | `str_projection_engine-7.4-RELEASE.jar` |
| 6 | `1f5ec5c2-3192-422c-8d1c-3d4c32c3ede5` | `application_1767175294768_64551` | `str_projection_engine-7.4-RELEASE.jar` |
| 7 | `6bd8e43a-77ad-4858-8a47-7d359f417125` | `application_1767175294768_64680` | `str_projection_engine-7.4-RELEASE.jar` |
| 8 | `312f80a6-e55a-4a4f-906e-da1b34fd123f` | `application_1767175294768_65097` | `str_projection_engine-7.4-RELEASE.jar` |
| 9 | `455d1972-4f3c-40cf-ba0f-8440fbc0912e` | `application_1767175294768_66555` | `str_projection_engine-7.4-RELEASE.jar` |
| 10 | `caf9eb29-ac82-4285-b0d0-282ca7ae471f` | `application_1767175294768_6655?` | `str_projection_engine-7.4-RELEASE.jar` |

> **⚠ Rows 5 and 6 carry the same `run_id` with two different `application_id` values.**
> If that read is correct, `run_id` is not unique in `run_history` — which matters directly
> for the partition story: two YARN applications writing under the same `runId=<uuid>`
> HDFS directory is exactly the shape that produces the nested/duplicated partition dirs.
> This is worth verifying first:
> ```sql
> SELECT run_id, COUNT(*) c, COUNT(DISTINCT application_id) a
> FROM dbprojection.run_history
> GROUP BY run_id HAVING COUNT(*) > 1
> ORDER BY c DESC;
> ```

---

## 4. Resolves an earlier open item

The clipped left-pane tree in the previous screenshots (`…18q3_dataprepara`, `…019q1_results`, …)
is now legible as a list of Hive databases / saved docs named:

```
recmigr_uz_stcreditrisk_2018q2_results
recmigr_uz_stcreditrisk_2018q3_dataprepara[tion]
recmigr_uz_stcreditrisk_2018q3_stresstest
recmigr_uz_stcreditrisk_2018q4_dataprepara[tion]
recmigr_uz_stcreditrisk_2018q4_results
recmigr_uz_stcreditrisk_2018q4_stresstest
recmigr_uz_stcreditrisk_2019q1_dataprepara[tion]
recmigr_uz_stcreditrisk_2019q1_results
recmigr_uz_stcreditrisk_2019q1_stresstest
recmigr_uz_stcreditrisk_2019q2_dataprepara[tion]
recmigr_uz_stcreditrisk_2019q2_results
…
```

Pattern: `recmigr_uz_stcreditrisk_<YYYY>q<N>_{dataprepara(tion)|results|stresstest}`.
`recmigr` = recette/migration; names are still right-clipped so the `dataprepara…` suffix
is unconfirmed.

---

## Flags / OCR confidence

- Photograph taken at an angle off a glossy screen; the background grid (section 3) is the
  lowest-confidence part. **Re-read those UUIDs from a real query before using them.**
- Row 10's `application_id` last digits are cut off by the viewport (`…_6655?`).
- Character pairs at risk throughout: `0`/`O`, `1`/`l`/`I`, `b`/`6`, `c`/`e`, `f`/`t`.
- The sample row is dated **2026-02-03**, whereas the `term_structure` partition dirs in the
  other brief are dated **2026-08-25 → 08-27**. Different runs; `dbddd6d8-…` will not be
  found among those partitions.
- Column list is complete and confirmed against the DDL (14 columns, `run_id` → `status`).
  There is no error-message or date-d'arrêté column on this table.
- DDL lines 23 / 29 / 36 are cut off at the right edge of the editor pane — the full
  `path`, `LOCATION` and `spark.sql.sources.schema` values were not captured.
