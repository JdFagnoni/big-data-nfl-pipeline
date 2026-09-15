# NFL Big Data Pipeline

An end-to-end batch data pipeline for NFL play-by-play and player-tracking data — NiFi ingestion into a zoned HDFS data lake, PySpark cleaning, a normalized Hive warehouse, and Superset/Jupyter dashboards answering five analytical questions.

## Overview

This is the final project ("Obligatorio") for **Herramientas de software para Big Data**, Universidad ORT Uruguay, Facultad de Ingeniería (July 2025, professors Eduardo Garcia and Alexis Arriola). It builds a full batch pipeline over the [Beginners Sports Analytics NFL Dataset](https://www.kaggle.com/datasets/aryashah2k/beginners-sports-analytics-nfl-dataset) (Kaggle): five related tables — `games`, `players`, `teams`, `plays` and `week_data` — covering the 2018 NFL regular season. Raw CSVs are ingested through Apache NiFi, staged across a four-zone HDFS data lake, cleaned and profiled with PySpark, modeled as a normalized relational schema exposed through Hive external tables, queried with Spark SQL, and visualized both inline in Jupyter (matplotlib/seaborn) and in an Apache Superset dashboard.

What makes it technically interesting is the mix of data granularity: `plays` and `games` give strategic/statistical context (down, yards to go, expected points added), while `week_data` is per-player, per-frame tracking data (speed, acceleration, distance, field position). Combining both lets the project ask cross-domain questions — e.g. which colleges produced the most physically demanding players, or which teams generated scoring-probability-positive plays week over week — instead of just aggregating box scores. The project also goes beyond a single "clean the CSV" notebook: it implements a realistic data lake lifecycle with an explicit immutable backup zone, a documented normalization decision, and deliberately mixed storage formats (Parquet and CSV) to exercise both.

As a second, self-contained deliverable ("Parte 2"), the team also designed — on paper, not implemented — an alternative modern stack for the same pipeline, swapping NiFi/HDFS/Hive/Superset for Apache Flume, Ceph (S3-compatible storage), Apache Iceberg (ACID/versioned tables), Trino and Redash, and compared the trade-offs of each swap stage by stage.

## Architecture / Approach

**Implemented pipeline (Part 1):**

```
5 raw CSVs (Kaggle)
   -> Apache NiFi flow "IngestaALnd" (GetFile -> Funnel -> PutHDFS)
   -> HDFS zone: landing (lnd)          -- files as received, no changes
   -> PySpark cleaning/profiling notebook
   -> HDFS zone: refined                -- deduped, typed, null-checked tables
   -> HDFS zone: raw                    -- untouched backup of original files
   -> Hive external tables (db: nfl_data), one per entity, pointing at /refined
   -> Spark SQL notebook answering 5 business questions
   -> HDFS zone: analytics (Parquet + CSV results)
   -> Hive tables over /analytics
   -> Visualization: Jupyter (pandas/matplotlib/seaborn) + Apache Superset dashboard
```

- **Ingestion**: NiFi process group `IngestaALnd` reads the 5 CSVs from a local VM folder (`/home/ort/nifi-input`) and writes them unmodified to HDFS landing.
- **Data lake zones**: `landing` (arrival), `raw` (immutable backup of originals, moved here once refined), `refined` (clean, deduplicated, type-cast tables used by Hive), `analytics` (query results and their Hive tables, feeding notebooks/dashboards).
- **Modeling**: a normalized entity-relationship model across the 5 tables (documented with draw.io), with explicit primary/foreign keys — e.g. `week_data` PK is `[nflId, frameId, gameId, playId]`, joined against `players`, `games`, `plays` and `teams`.
- **Serving layer**: Hive external tables in database `nfl_data`, each pointing directly at its `/refined/<table>` HDFS path (`TEXTFILE`, comma-delimited).
- **Analysis**: a second notebook answers 5 questions via Spark SQL over the Hive tables, saving each result under `/analytics` — 4 in Parquet, 1 in CSV (to exercise both storage formats).
- **Visualization**: 3 of the 5 answers are charted inline in the notebook (seaborn heatmap, matplotlib bar chart, matplotlib line chart); 3 are charted in a Superset dashboard (Country Map, Bar Chart, Pie Chart) backed by dedicated Hive tables over `/analytics`.

**Alternative architecture (Part 2 — proposed and documented only, not implemented):**

```
Sources (API/CSV/logs/IoT) -> Apache Flume -> Ceph (S3-compatible, landing zone)
   -> Apache Spark (PySpark) -> Ceph (refined zone) -> Apache Iceberg (ACID, versioned tables)
   -> Trino (distributed SQL) -> Redash (dashboards)
```

## Key results

All figures below are transcribed directly from the Hive/Spark query output and charts documented in the project report.

- **Dataset scale**: `week_data.csv` (the most granular table) is ~118.6 MB; `plays.csv` ~4.98 MB; `players.csv` ~71 KB; `games.csv` ~10 KB; `teams.csv` <1 KB. Season covered: 2018 NFL regular season, weeks 1–17.
- **Q1 — States with the most games played**: California leads with **62** games; NFL games in this dataset are spread across 22 US states.
- **Q3 — Highest average speed by offensive position, successful pass plays**: TE 6.77 m/s, WR 5.48 m/s, FB 4.98 m/s, RB 4.57 m/s, QB 2.04 m/s.
- **Q4 — Teams allowing the fewest yards per play on defense** (top 5, average yards allowed): BAL 5.30, CHI 5.34, MIN 5.56, BUF 5.63, ARI 5.85.
- **Q5 — Colleges behind the top 10 players by total distance covered in the season** (summed distance in yards, top 5 colleges): Alabama 7,281.32, Georgia 5,436.56, Florida State 5,188.99, Notre Dame 5,140.69, Ohio State 5,117.87.
- **Q2 — Weekly count of offensive plays with positive EPA (Expected Points Added) by team**: values reach up to 33 in a single team-week (e.g. PIT in week 2), visualized as a team × week heatmap across all 17 weeks.

## Design decisions

**1. Normalized relational model instead of a denormalized/flattened layer.**
The five source tables already carry a clear relational structure with well-defined unique keys (`nflId`, `gameId`, `playId`, `teamAbbreviation`), so the team kept them normalized rather than pre-joining or flattening them into a wide analytics table or star schema before loading into Hive. This preserves data integrity, avoids redundant storage, and lets every question be answered with a plain SQL join. The discarded alternative — a denormalized fact table over `week_data` with all dimensions embedded — was judged unnecessary added ETL complexity: at this dataset's size there was no query-performance problem that justified paying for the redundancy.

**2. A dedicated, never-modified `raw` zone, separate from `landing`.**
After NiFi lands files in `landing/` and Spark reads and cleans them into `refined/`, the originals are moved out of `landing/` into a separate `raw/` zone that is treated as permanent, untouched storage. `landing/` itself stays a transient staging area for whatever arrives next. This was decided so that adding a new source table later, or reprocessing everything from scratch, can always start from a clean, unaltered copy — without the risk of new incoming files getting mixed up with data that has already been cleaned and moved to `refined/`. The discarded alternative was to treat `landing/` itself as the permanent raw backup, which was rejected because it would conflate an active staging area with a preservation zone.

## Tech stack

- **Ingestion**: Apache NiFi (`GetFile`, `PutHDFS`, `Funnel` processors)
- **Distributed storage**: HDFS (Hadoop)
- **Data warehouse / catalog**: Apache Hive (external tables)
- **Processing / query engine**: Apache Spark — PySpark, Spark SQL
- **Visualization**: Apache Superset (Country Map, Bar Chart, Pie Chart); Jupyter Notebook
- **Python libraries** (notebooks): pandas, matplotlib, seaborn
- **Modeling**: draw.io (entity-relationship diagram)
- **Provisioning**: WinSCP (local → VM file transfer)
- **Proposed alternative stack** (Part 2, design-only — not implemented): Apache Flume, Ceph (RADOS Gateway, S3-compatible), Apache Iceberg, Trino, Redash

## How to run

This repository, as currently committed, contains the project's written report (`NFL Data Pipeline Documentation.pdf`) rather than a runnable codebase — the two Jupyter notebooks the report describes (`analisisYRefinamiento_De_Datos` and `consultas_hive_nfl`) are not included here.

**To review the work:**
1. Read `NFL Data Pipeline Documentation.pdf` in the repo root — it walks through every step (NiFi flow configuration, HDFS commands, Hive DDL, Spark SQL queries, and all chart/dashboard outputs) with terminal and UI screenshots.
2. The source dataset is on Kaggle: https://www.kaggle.com/datasets/aryashah2k/beginners-sports-analytics-nfl-dataset

**To reproduce the pipeline from scratch** (notebooks referenced below are not part of this repository):
1. Provision a machine (or VM) with Hadoop (HDFS), Hive, Spark and NiFi installed, plus Superset for the dashboard.
2. Create the four HDFS zones: `hdfs dfs -mkdir -p /user/bigdata/zones/{lnd,raw,refined,analytics}`.
3. Run the NiFi `IngestaALnd` flow to land the 5 source CSVs into `/user/bigdata/zones/lnd`.
4. `pip install -r requirements.txt`, then run `analisisYRefinamiento_De_Datos.ipynb` (PySpark kernel) to produce the cleaned tables under `/refined`.
5. Create the Hive external tables over `/refined` (DDL documented in the PDF), then run `consultas_hive_nfl.ipynb` to answer the 5 questions and populate `/analytics`.
6. Point Superset at the `nfl_data` Hive database to rebuild the dashboard.

## Team

Team project (2 members): **Juan Diego Fagnoni** and **Francisco Baraibar**. The design, implementation and analysis were carried out jointly, as documented in the project report; this repository is maintained by Juan Diego Fagnoni.
