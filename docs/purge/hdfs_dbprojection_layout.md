# HDFS layout snapshot — `dbprojection.db` (STCreditRisk_STE)

Source: Hue / CDP "Navigateur de fichiers", two screenshots taken 29/08/2026 15:42 (FRA locale).
Transcription is verbatim from the UI; see **Flags** for low-confidence reads.

---

## 1. Database root

**Path:** `/Projects/STCreditRisk_STE/hive/databases/dbprojection.db`

| Nom | Utilisateur | Groupe | Autorisations | Date |
|---|---|---|---|---|
| `..` | usertest_migration | supergroup | `drwx------` | July 13, 2026 11:33 AM |
| `.` | usertest_migration | supergroup | `drwx------` | August 03, 2026 03:24 PM |
| `run_history` | sttengineihm | supergroup | `drwx------` | August 27, 2026 03:53 PM |
| `lgd_term_structure_detailed` | sttengineihm | supergroup | `drwx------` | August 27, 2026 03:53 PM |
| `lgd` | sttengineihm | supergroup | `drwx------` | August 27, 2026 03:53 PM |
| `pcure` | sttengineihm | supergroup | `drwx------` | August 27, 2026 03:53 PM |
| `projected_cr_detailed` | sttengineihm | supergroup | `drwx------` | August 27, 2026 03:53 PM |
| `chr_idealised_detailed` | sttengineihm | supergroup | `drwx------` | August 27, 2026 03:52 PM |
| `chr_detailed` | usertest_migration | supergroup | `drwx------` | August 27, 2026 03:52 PM |
| `term_structure` | usertest_migration | supergroup | `drwx------` | August 27, 2026 03:52 PM |
| `term_structure_idealised_detailed` | sttengineihm | supergroup | `drwx------` | August 27, 2026 03:52 PM |
| `term_structure_detailed` | usertest_migration | supergroup | `drwx------` | August 27, 2026 03:52 PM |
| `migration_matrix` | usertest_migration | supergroup | `drwx------` | August 27, 2026 03:51 PM |
| `migration_matrix_detailed` | usertest_migration | supergroup | `drwx------` | August 27, 2026 03:51 PM |
| `projected_z` | usertest_migration | supergroup | `drwx------` | August 27, 2026 03:48 PM |
| `projected_z_detailed` | usertest_migration | supergroup | `drwx------` | August 27, 2026 03:48 PM |
| `scenarii_ponderation` | usertest_migration | supergroup | `drwx------` | August 27, 2026 03:48 PM |

> Listing is cut off at `scenarii_ponderation` (bottom of viewport). More entries exist below.

**Ownership split worth noting:** the tables written by the engine under the IHM service account are owned by
`sttengineihm`; the ones touched by the migration are owned by `usertest_migration`. `term_structure`,
`term_structure_detailed`, `chr_detailed`, `migration_matrix*`, `projected_z*`, `scenarii_ponderation`
→ `usertest_migration`. `run_history`, `lgd*`, `pcure`, `projected_cr_detailed`,
`chr_idealised_detailed`, `term_structure_idealised_detailed` → `sttengineihm`.

---

## 2. `term_structure` partition directories

**Path:** `/Projects/STCreditRisk_STE/hive/databases/dbprojection.db/term_structure`

Header counter in left pane reads `(185)`.

All partition dirs use the **capital-I spelling `runId=`**. Owner `sttengineihm`, group `supergroup`,
perms `drwx------` on every row.

| Nom | Date |
|---|---|
| `..` (owner `usertest_migration`) | August 03, 2026 03:24 PM |
| `.` (owner `usertest_migration`) | August 27, 2026 03:52 PM |
| `runId=9df8cf3a-c2fa-4bfd-9068-aa6587ba84cf` | August 27, 2026 03:52 PM |
| `runId=5c1f53c8-6afe-41d6-9a5e-9953219cde05` | August 26, 2026 03:22 PM |
| `runId=bee7fda7-382e-45ad-a4ac-813e43e38bf2` | August 26, 2026 12:51 PM |
| `runId=18d71614-7574-4aaa-a550-0226bf2f32b3` | August 26, 2026 10:45 AM |
| `runId=323d96d1-7d98-4634-8371-ec3e7d934eb8` | August 25, 2026 04:34 PM |
| `runId=f4a22268-44e3-4d35-a63a-be119070f2f2` | August 25, 2026 04:34 PM |
| `runId=4dbe3dcf-99f9-4af5-840a-5a78cdea2600` | August 25, 2026 04:34 PM |
| `runId=370ea3d2-c83d-4192-a7ba-9a9ea9b05187` | August 25, 2026 04:05 PM |
| `runId=3eb40be8-1ea7-4ba6-865c-8d7c90143b3e` | August 25, 2026 03:54 PM |
| `runId=e95e9c75-2617-4107-8e65-9f3f27d445ee` | August 25, 2026 03:51 PM |
| `runId=f7f5bade-c973-4e19-8a13-b1ccf58159c1` | August 25, 2026 03:51 PM |
| `runId=46604247-bca6-488a-9ad8-adc99b50bb57` | August 25, 2026 03:51 PM |
| `runId=b5316509-4278-4fb3-8418-f58433af7b51` | August 25, 2026 03:50 PM |
| `runId=a7558251-7205-422b-89fe-63ded7244f6b` | August 25, 2026 03:00 PM |
| `runId=d78b1fa6-c21b-402e-909e-98d54d6ee477` | August 25, 2026 02:56 PM |

> Listing cut off at the bottom row; `(185)` suggests ~183 partition dirs in total.

**Note:** the screenshot only covers the first level. The double-nesting
(`runId=<uuid>/runid=<uuid>/`) reported earlier would appear one level deeper and is **not**
visible/confirmed in this capture. Needs `hdfs dfs -ls` one level down to verify.

---

## 3. Left-pane tree fragment (partially visible, both shots)

Truncated directory names visible in the collapsed left sidebar, apparently a per-quarter run tree:

```
…18q2_results
…18q3_dataprepara[tion]
…18q3_stresstest
…18q4_dataprepara[tion]
…18q4_results
…18q4_stresstest
…019q1_dataprepara[tion]
…019q1_results
…019q1_stresstest
…019q2_dataprepara[tion]
…019q2_results
…019q2_stresstest
```

Pattern is `<YYYY>q<N>_{dataprepara(tion),results,stresstest}`, i.e. `2018q2` … `2019q2`.
Prefixes and full names are clipped by the pane width — not reliable.

---

## Flags / OCR confidence

- **Partition key casing is `runId=` (capital I) on disk** for every row in `term_structure`.
  This is the directly relevant fact for the writer-casing bug — confirm what the DataFrame column
  is actually named (`runid` vs `runId`) before generating `ALTER TABLE … ADD PARTITION` statements,
  since Hive lowercases column names in the metastore.
- UUIDs were read from a photographed screen at an angle. Characters at risk of confusion:
  `0`/`O`, `1`/`l`, `b`/`6`, `e`/`c`. **Do not use these UUIDs as literals in any script** —
  re-derive them from `hdfs dfs -ls` output.
- `Taille` column is empty for all rows (directories).
- Permissions render as `drwx——` in the screenshot (UI collapses the dashes); read as `drwx------`.
- The `(185)` counter in the left pane is partially clipped (`(185)` in shot 1, `5)` in shot 2);
  it may be `185` or a longer number truncated on the left.
- No `_SUCCESS`, `.hive-staging`, or file-level entries visible at either level.

## Suggested next commands (to replace this transcription with ground truth)

```bash
BASE=/Projects/STCreditRisk_STE/hive/databases/dbprojection.db

# full partition listing, level 1
hdfs dfs -ls "$BASE/term_structure" | awk '{print $NF}' > /tmp/ts_level1.txt
wc -l /tmp/ts_level1.txt

# detect double nesting
hdfs dfs -ls "$BASE/term_structure/*/" | grep -iE 'run_?id=' | head -50

# same for the detailed table
hdfs dfs -ls "$BASE/term_structure_detailed" | head -20

# compare with what the metastore knows
# (beeline) SHOW PARTITIONS dbprojection.term_structure;
```
