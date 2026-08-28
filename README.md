# Data Modeling with Apache Cassandra

**Udacity Data Engineering Nanodegree — Project 1**

A fully query-driven NoSQL database built with Apache Cassandra for **Sparkify**, a music streaming startup that needs to analyze what songs its users are listening to. The project covers end-to-end data engineering: raw CSV ingestion, ETL denormalization, and schema design tailored to three specific analytical queries.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Dataset](#dataset)
- [Schema Design](#schema-design)
- [ETL Pipeline](#etl-pipeline)
- [Project Structure](#project-structure)
- [Technologies](#technologies)
- [How to Run](#how-to-run)
- [Query Results](#query-results)

---

## Overview

Sparkify's user activity lives in a directory of date-partitioned CSV files. The analytics team wants answers to three specific questions — but there is no database to query against. This project solves that by:

1. **Consolidating** raw CSV files into a single, clean denormalized dataset (`event_datafile_new.csv`).
2. **Modeling** three Apache Cassandra tables, each shaped around one analytical query.
3. **Loading** data and verifying results with `SELECT` statements — no `ALLOW FILTERING`.

The core principle behind the design is **query-first modeling**: in Cassandra, the schema follows the query, not the other way around.

---

## Architecture

```
event_data/
├── 2018-11-08-events.csv
├── 2018-11-11-events.csv       ─┐
└── 2018-11-29-events.csv        │  Raw date-partitioned
         │                       │  logs (all user events)
         ▼                      ─┘
  ┌─────────────────────┐
  │   ETL Pipeline      │  Filter → NextSong events only
  │   (Part I)          │  Select 11 relevant columns
  └────────┬────────────┘
           │
           ▼
  event_datafile_new.csv  (denormalized, 11 columns)
           │
           ▼
  ┌─────────────────────────────────────────────────┐
  │               Apache Cassandra                  │
  │               Keyspace: sparkifydb              │
  │                                                 │
  │  song_plays_by_session       (Query 1)          │
  │  song_plays_by_user_session  (Query 2)          │
  │  users_by_song               (Query 3)          │
  └─────────────────────────────────────────────────┘
```

---

## Dataset

**Source:** `event_data/` — CSV files partitioned by date, each covering one day of user activity on the Sparkify platform.

**Raw columns per file:**

| Column | Description |
|--------|-------------|
| `artist` | Recording artist name |
| `auth` | Authentication state (`Logged In`, `Logged Out`, `Guest`) |
| `firstName` | User first name |
| `gender` | User gender |
| `itemInSession` | Sequential event index within the session |
| `lastName` | User last name |
| `length` | Song duration in seconds |
| `level` | Subscription tier (`free` / `paid`) |
| `location` | User's metropolitan area |
| `method` | HTTP method (`GET` / `PUT`) |
| `page` | App page visited (e.g., `NextSong`, `Home`, `Settings`) |
| `registration` | User registration timestamp |
| `sessionId` | Unique session identifier |
| `song` | Song title |
| `status` | HTTP status code |
| `ts` | Event timestamp (ms) |
| `userId` | Unique user identifier |

Only rows where `page == 'NextSong'` are loaded into Cassandra.

---

## Schema Design

Each table is designed around a single query. The `PRIMARY KEY` is derived directly from the `WHERE` and `ORDER BY` clauses of that query.

---

### Table 1 — `song_plays_by_session`

**Query:** What artist, song, and duration was played during `sessionId = 338` at `itemInSession = 4`?

```sql
SELECT artist, song, length
FROM song_plays_by_session
WHERE sessionId = 338 AND itemInSession = 4;
```

```
song_plays_by_session
─────────────────────────────────────────
 sessionId     INT     ← partition key
 itemInSession INT     ← clustering key
 artist        TEXT
 song          TEXT
 length        DECIMAL
```

**Key rationale:**
- `sessionId` as the partition key groups all events from a session on a single node.
- `itemInSession` as the clustering column narrows the lookup to the exact event position without `ALLOW FILTERING`.

---

### Table 2 — `song_plays_by_user_session`

**Query:** What artist, song (ordered by position), and user listened during `userId = 10`, `sessionId = 182`?

```sql
SELECT artist, song, firstName, lastName
FROM song_plays_by_user_session
WHERE userId = 10 AND sessionId = 182;
```

```
song_plays_by_user_session
──────────────────────────────────────────────────
 userId        INT     ← composite partition key
 sessionId     INT     ← composite partition key
 itemInSession INT     ← clustering key (auto-sorts)
 artist        TEXT
 song          TEXT
 firstName     TEXT
 lastName      TEXT
```

**Key rationale:**
- The composite partition key `(userId, sessionId)` co-locates all events from a user's session on one node, since both columns appear in the `WHERE` clause.
- `itemInSession` as the clustering column ensures results are returned in playback order automatically — no `ORDER BY` needed.

---

### Table 3 — `users_by_song`

**Query:** Which users listened to `'All Hands Against His Own'`?

```sql
SELECT firstName, lastName
FROM users_by_song
WHERE song = 'All Hands Against His Own';
```

```
users_by_song
───────────────────────────────
 song      TEXT   ← partition key
 userId    INT    ← clustering key
 firstName TEXT
 lastName  TEXT
```

**Key rationale:**
- `song` as the partition key puts all listeners of the same song on one node — the lookup is a single-partition scan.
- `userId` as the clustering column ensures one row per unique user, even if the same user listened across multiple sessions (upsert deduplication).

---

## ETL Pipeline

The pipeline runs in two parts inside the Jupyter notebook.

**Part I — Pre-processing:**
1. Walk all CSV files in `event_data/` using `glob`.
2. Aggregate every row into a Python list.
3. Filter to `page == 'NextSong'` events only.
4. Write the 11 selected columns to `event_datafile_new.csv`.

**Part II — Cassandra ingestion:**
1. Connect to a local Cassandra cluster (`127.0.0.1`).
2. Create (or reuse) the `sparkifydb` keyspace with `SimpleStrategy`.
3. For each query:
   - `DROP TABLE IF EXISTS` (clean slate for re-runs).
   - `CREATE TABLE IF NOT EXISTS` with the designed PRIMARY KEY.
   - Stream rows from `event_datafile_new.csv` and `INSERT` each record.
   - Run the target `SELECT` and display results as a pandas DataFrame.
4. Drop all tables and close the connection cleanly.

---

## Project Structure

```
aws_data-modeling-cassandra/
│
├── event_data/
│   ├── 2018-11-08-events.csv      # Day-level raw event logs
│   ├── 2018-11-11-events.csv
│   └── 2018-11-29-events.csv
│
├── Project_1B_Project_Template.ipynb   # ETL pipeline + Cassandra data model
├── event_datafile_new.csv              # Generated by the notebook (Part I)
└── README.md
```

---

## Technologies

| Tool | Role |
|------|------|
| **Apache Cassandra** | Distributed NoSQL database |
| **Python 3** | ETL scripting and orchestration |
| **cassandra-driver** | Python client for Cassandra |
| **pandas** | Query result formatting and display |
| **Jupyter Notebook** | Interactive development environment |

---

## How to Run

### Prerequisites

- Python 3.7+
- Apache Cassandra running locally on port `9042`
- Python packages: `cassandra-driver`, `pandas`

```bash
pip install cassandra-driver pandas jupyter
```

### Steps

1. **Clone the repository:**
   ```bash
   git clone https://github.com/LuccaHiratsuca/aws_data-modeling-cassandra.git
   cd aws_data-modeling-cassandra
   ```

2. **Start Apache Cassandra** (if not already running):
   ```bash
   cassandra -f
   ```

3. **Launch Jupyter Notebook:**
   ```bash
   jupyter notebook Project_1B_Project_Template.ipynb
   ```

4. **Run all cells** in order — Part I creates `event_datafile_new.csv`, Part II builds the Cassandra tables and validates each query.

---

## Query Results

### Query 1 — `sessionId = 338`, `itemInSession = 4`

| Artist | Song | Length (s) |
|--------|------|-----------|
| Faithless | Music Matters (Mark Knight Dub) | 495.3073 |

### Query 2 — `userId = 10`, `sessionId = 182`

| Artist | Song | First Name | Last Name |
|--------|------|-----------|-----------|
| Down To The Bone | Keep On Keepin' On | Sylvie | Cruz |
| Three Drives | Greece 2000 | Sylvie | Cruz |
| Sebastien Tellier | Kilometer | Sylvie | Cruz |
| Lonnie Gordon | Catch You Baby (Steve Pitron & Max Sanna Radio Edit) | Sylvie | Cruz |

### Query 3 — `song = 'All Hands Against His Own'`

| First Name | Last Name |
|-----------|-----------|
| Jacqueline | Lynch |
| Sara | Johnson |
| Tegan | Levine |
