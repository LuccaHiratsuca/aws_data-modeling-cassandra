<h1 align="center">Data Modeling with Apache Cassandra</h1>

<p align="center">
  <em>A query-driven NoSQL analytics database for Sparkify, a music streaming startup.</em>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Apache%20Cassandra-1287B1?style=flat-square&logo=apachecassandra&logoColor=white" alt="Apache Cassandra">
  <img src="https://img.shields.io/badge/Python%203-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python 3">
  <img src="https://img.shields.io/badge/pandas-150458?style=flat-square&logo=pandas&logoColor=white" alt="pandas">
  <img src="https://img.shields.io/badge/Jupyter-F37626?style=flat-square&logo=jupyter&logoColor=white" alt="Jupyter">
  <img src="https://img.shields.io/badge/CQL-no%20ALLOW%20FILTERING-success?style=flat-square" alt="No ALLOW FILTERING">
</p>

---

## Table of Contents

- [Objective](#objective)
- [Architecture](#architecture)
- [Dataset](#dataset)
- [Data Modeling Approach](#data-modeling-approach)
- [Schema Design](#schema-design)
- [ETL Pipeline](#etl-pipeline)
- [Query Results](#query-results)
- [How to Run](#how-to-run)
- [Project Structure](#project-structure)
- [Design Notes](#design-notes)

---

## Objective

Sparkify collects user activity from its music streaming app as a directory of date-partitioned CSV files. The analytics team wants to know **what songs users are listening to**, but there is no database to query — the data sits inert on disk, and answering even a simple question means writing a bespoke script that scans every file.

This project delivers the missing layer: an Apache Cassandra database purpose-built to serve three specific analytical questions.

| # | Business question |
|:-:|---|
| 1 | What artist, song title, and song length was heard during a given session at a given position? |
| 2 | What artist, song (in playback order), and user name correspond to a given user's session? |
| 3 | Which users listened to a particular song? |

Delivering this means three things:

1. **Consolidate** 30 daily CSV files into one clean, denormalized dataset.
2. **Model** three Cassandra tables, each shaped around exactly one of the queries above.
3. **Load and validate** — every question answered by a single-partition read, with **no `ALLOW FILTERING`**.

> The guiding constraint is that **Cassandra has no joins and no ad-hoc `WHERE` clauses**. You cannot filter on a column that is not part of the primary key. So the schema cannot be designed first and queried later — the query comes first, and the table is built to serve it. Three questions therefore mean three tables, and the same row is deliberately written more than once.

---

## Architecture

```
             ┌──────────────────────────────────────────────┐
   SOURCE    │  event_data/                                 │
             │    2018-11-01-events.csv                     │
             │    2018-11-02-events.csv    30 daily files   │
             │            ...              8,056 raw rows   │
             │    2018-11-30-events.csv                     │
             └───────────────────────┬──────────────────────┘
                                     │
                                     ▼
             ┌──────────────────────────────────────────────┐
    PART I   │  ETL — Pre-processing                        │
             │    • walk every CSV via glob                 │
             │    • keep only page == 'NextSong'            │
             │    • project the 11 modeling columns         │
             └───────────────────────┬──────────────────────┘
                                     │
                                     ▼
   STAGING     event_datafile_new.csv  ·  6,820 rows × 11 cols
                                     │
                     ┌───────────────┼───────────────┐
                     │               │               │
                     ▼               ▼               ▼
             ┌──────────────────────────────────────────────┐
   PART II   │        Apache Cassandra — sparkifydb         │
             ├──────────────────────────────────────────────┤
             │ song_plays_by_session         Q1   6,820 rows│
             │   PK (sessionId, itemInSession)              │
             ├──────────────────────────────────────────────┤
             │ song_plays_by_user_session    Q2   6,820 rows│
             │   PK ((userId, sessionId), itemInSession)    │
             ├──────────────────────────────────────────────┤
             │ users_by_song                 Q3   6,618 rows│
             │   PK (song, userId)                          │
             └──────────────────────────────────────────────┘
```

The same staging file feeds all three tables. This **controlled denormalization** is the core Cassandra trade-off: storage is cheap and writes are fast, so duplicating data across purpose-built tables is preferable to the cross-node scatter a join would require.

---

## Dataset

Thirty daily CSV files covering **2018-11-01 → 2018-11-30**, one per day of user activity.

### Raw event schema — 17 columns

| Column | Description | Column | Description |
|---|---|---|---|
| `artist` | Recording artist | `page` | App page (`NextSong`, `Home`, …) |
| `auth` | Auth state (`Logged In`/`Out`) | `registration` | Registration timestamp |
| `firstName` | User first name | `sessionId` | Session identifier |
| `gender` | User gender | `song` | Song title |
| `itemInSession` | Position within the session | `status` | HTTP status code |
| `lastName` | User last name | `ts` | Event timestamp (ms) |
| `length` | Song duration (seconds) | `userId` | User identifier |
| `level` | Subscription tier | `method` | HTTP method |
| `location` | User metropolitan area | | |

### Profile after filtering to `NextSong`

| Metric | Value |
|---|---|
| Raw event rows | 8,056 |
| Song plays (`page == 'NextSong'`) | **6,820** |
| Distinct users | 96 |
| Distinct sessions | 776 |
| Distinct songs | 5,190 |
| Distinct artists | 3,148 |
| Distinct locations | 63 |
| Subscription split | 5,591 paid · 1,229 free |
| Mean song length | 247.03 s |
| Most played artist | Coldplay (58 plays) |

Only `NextSong` events are loaded — the remaining 1,236 rows are navigation events (`Home`, `Settings`, `Logout`, …) that carry no song and are irrelevant to every query.

---

## Data Modeling Approach

Each table was derived mechanically from its query, in four steps:

1. **Read the `WHERE` clause** → these columns must form the partition key, so the read touches exactly one node.
2. **Read the `ORDER BY` requirement** → this column becomes a clustering column, since Cassandra physically sorts rows within a partition by clustering key.
3. **Check uniqueness** → add clustering columns until the primary key identifies a single row. Cassandra silently upserts on primary-key collision, so an under-specified key means **silent data loss**.
4. **Name the table after the access pattern**, not the entity — `users_by_song`, not `songs`.

Column order in each `CREATE TABLE` mirrors the key structure: partition key columns first, then clustering columns, then the payload.

---

## Schema Design

### Query 1 — `song_plays_by_session`

> Artist, song title, and length for `sessionId = 338`, `itemInSession = 4`.

```sql
CREATE TABLE IF NOT EXISTS song_plays_by_session (
    sessionId     INT,
    itemInSession INT,
    artist        TEXT,
    song          TEXT,
    length        DOUBLE,
    PRIMARY KEY (sessionId, itemInSession)
);

SELECT artist, song, length
FROM song_plays_by_session
WHERE sessionId = 338 AND itemInSession = 4;
```

**`PRIMARY KEY (sessionId, itemInSession)`**

- **Partition key `sessionId`** — the query filters on session, so co-locating every event of a session on one node turns the lookup into a single-partition read.
- **Clustering column `itemInSession`** — satisfies the second equality predicate without `ALLOW FILTERING`, and pins the read to one row rather than a partition scan.
- **Uniqueness** — a session never reuses a position, so the pair is unique. Verified empirically: 6,820 plays produce 6,820 rows, so no insert overwrote another.
- `length` is `DOUBLE` because the driver binds a Python `float`; `DECIMAL` would expect a `decimal.Decimal` and store the value imprecisely.

---

### Query 2 — `song_plays_by_user_session`

> Artist, song (in playback order), and user name for `userId = 10`, `sessionId = 182`.

```sql
CREATE TABLE IF NOT EXISTS song_plays_by_user_session (
    userId        INT,
    sessionId     INT,
    itemInSession INT,
    artist        TEXT,
    song          TEXT,
    firstName     TEXT,
    lastName      TEXT,
    PRIMARY KEY ((userId, sessionId), itemInSession)
);

SELECT artist, song, firstName, lastName
FROM song_plays_by_user_session
WHERE userId = 10 AND sessionId = 182;
```

**`PRIMARY KEY ((userId, sessionId), itemInSession)`**

- **Composite partition key `(userId, sessionId)`** — the query constrains both columns, so both belong in the partition key. The extra parentheses matter: written as `PRIMARY KEY (userId, sessionId, itemInSession)`, `userId` alone would be the partition key and `sessionId` a clustering column. That still answers this query, but it partitions by user rather than by user-session, so a heavy listener's every play piles into one wide partition.
- **Clustering column `itemInSession`** — Cassandra stores rows sorted ascending by clustering key, so **playback order comes back for free**; no `ORDER BY` is needed in the `SELECT`.
- **Ordering guarantee** — results are returned in playback order by construction, which is precisely what the question asks for.

---

### Query 3 — `users_by_song`

> Every user who listened to `'All Hands Against His Own'`.

```sql
CREATE TABLE IF NOT EXISTS users_by_song (
    song      TEXT,
    userId    INT,
    firstName TEXT,
    lastName  TEXT,
    PRIMARY KEY (song, userId)
);

SELECT firstName, lastName
FROM users_by_song
WHERE song = 'All Hands Against His Own';
```

**`PRIMARY KEY (song, userId)`**

- **Partition key `song`** — the only filter is the song title, so partitioning by it makes the lookup a single-partition scan instead of the full-table scan that `WHERE song = ?` would demand on a table keyed by anything else.
- **Clustering column `userId`** — this is the load-bearing choice. Without it, `song` alone would be the whole primary key and **every listener after the first would overwrite the previous one**, leaving exactly one row per song. Adding `userId` makes the key unique per listener.
- **Deduplication is a feature here** — a user who played the same song in several sessions upserts onto the same primary key, so each listener appears once. This is why the table holds **6,618 rows rather than 6,820**: 202 repeat plays collapse into their existing rows, which is exactly the "which *users*" semantics the question wants.

---

## ETL Pipeline

**Part I — Pre-processing**

1. Discover every `event_data/*.csv` via `glob`.
2. Read all 30 files, skipping headers, into one in-memory row list (8,056 rows).
3. Filter to `page == 'NextSong'`.
4. Project the 11 modeling columns and write `event_datafile_new.csv` (6,820 rows) using a `QUOTE_ALL` dialect, so titles containing commas survive the round trip.

**Part II — Modeling and ingestion**

1. Connect to the local cluster and create the `sparkifydb` keyspace (`SimpleStrategy`, `replication_factor = 1` — appropriate for a single-node dev cluster, not production).
2. For each of the three queries: `DROP TABLE IF EXISTS` → `CREATE TABLE IF NOT EXISTS` → stream inserts from the staging file → run the `SELECT` → render via pandas.
3. Drop all tables and shut the connection down cleanly.

`DROP` before `CREATE` makes the notebook **idempotent**: it can be re-run end to end without stale rows from a previous pass.

---

## Query Results

### Query 1 — `sessionId = 338`, `itemInSession = 4`

| Artist | Song | Length (s) |
|---|---|---|
| Faithless | Music Matters (Mark Knight Dub) | 495.3073 |

### Query 2 — `userId = 10`, `sessionId = 182`

Returned in playback order via the `itemInSession` clustering column:

| # | Artist | Song | First Name | Last Name |
|:-:|---|---|---|---|
| 0 | Down To The Bone | Keep On Keepin' On | Sylvie | Cruz |
| 1 | Three Drives | Greece 2000 | Sylvie | Cruz |
| 2 | Sebastien Tellier | Kilometer | Sylvie | Cruz |
| 3 | Lonnie Gordon | Catch You Baby (Steve Pitron & Max Sanna Radio Edit) | Sylvie | Cruz |

### Query 3 — `song = 'All Hands Against His Own'`

| User ID | First Name | Last Name |
|:-:|---|---|
| 29 | Jacqueline | Lynch |
| 80 | Tegan | Levine |
| 95 | Sara | Johnson |

> **How these were produced.** The environment used to prepare this repository had no Cassandra server available, so the notebook was not executed against a live cluster. The figures above were instead produced by [`verify_etl.py`](verify_etl.py), which runs the real Part I ETL over `event_data/` and then resolves each query using the same partition-key and clustering-column semantics its Cassandra table enforces — including the upsert behaviour that gives Query 3 its deduplication. Run `python verify_etl.py` to reproduce every number quoted in this README. Executing the notebook against a running cluster is expected to yield the same output; that has not been confirmed here.

---

## How to Run

### Prerequisites

- Python 3.7+
- Apache Cassandra reachable on `127.0.0.1:9042`

```bash
pip install cassandra-driver pandas jupyter
```

### Steps

```bash
git clone https://github.com/LuccaHiratsuca/aws_data-modeling-cassandra.git
cd aws_data-modeling-cassandra

# 1 — start Cassandra (or: docker run -d -p 9042:9042 --name cassandra cassandra:4.1)
cassandra -f

# 2 — run the full pipeline and the three queries
jupyter notebook Project_1B_Project_Template.ipynb
```

Run the cells in order: Part I writes `event_datafile_new.csv`, Part II builds the tables and validates each query.

To check the ETL and expected results **without** a Cassandra cluster:

```bash
python verify_etl.py
```

---

## Project Structure

```
aws_data-modeling-cassandra/
├── event_data/                          # 30 daily CSVs (2018-11-01 → 2018-11-30)
│   ├── 2018-11-01-events.csv
│   ├── ...
│   └── 2018-11-30-events.csv
├── Project_1B_Project_Template.ipynb    # ETL pipeline + Cassandra data model
├── verify_etl.py                        # Cluster-free ETL and query verification
├── event_datafile_new.csv               # Generated by Part I (gitignored)
├── .gitignore
└── README.md
```

---

## Design Notes

**Why three tables for three questions.** Cassandra rejects a `WHERE` clause on any non-key column unless you add `ALLOW FILTERING`, which triggers a full cluster scan and does not scale. One table per access pattern is the idiomatic answer, and duplicated data is the accepted cost.

**Partition sizing.** Partitioning Query 2 by `(userId, sessionId)` rather than `userId` alone keeps partitions small and evenly distributed. In this dataset `sessionId` never spans users — 776 sessions map to exactly 776 user-session pairs — so the composite key is narrower without losing anything.

**When upsert is a bug vs. a feature.** The same mechanic serves opposite purposes across these tables. In `users_by_song` the upsert on `(song, userId)` is what produces one row per listener. In `song_plays_by_session` an under-specified key would have silently destroyed rows — the 6,820-in / 6,820-out row count is the check that it did not.

**Replication.** `SimpleStrategy` with `replication_factor = 1` suits a single-node development cluster. A production deployment would use `NetworkTopologyStrategy` with a replication factor of at least 3 per datacenter.

**Known limitation.** Inserts are issued one row at a time with `session.execute()` — roughly 20,000 synchronous round trips across the three tables. Prepared statements with `execute_async()` and batched futures would cut load time substantially. Left as-is here for readability, since the dataset is small.

---

<p align="center"><sub>Udacity Data Engineering Nanodegree · Project 1B — Data Modeling with Apache Cassandra</sub></p>
