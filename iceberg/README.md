# Iceberg Learning Project

A hands-on pipeline to learn Apache Iceberg — ingest CSV data into S3, transform it, and query with DuckDB.

**Stack:** PyIceberg · PyArrow · DuckDB · SQLite catalog · S3 storage

---

## How it works

```
CSV file → Iceberg (raw) → Iceberg (enriched + partitioned) → DuckDB query
```

1. **Ingest** — reads a CSV and appends it to an Iceberg table on S3. Every run adds a new snapshot (history is preserved).
2. **Transform** — reads the raw table, adds an `email_domain` column, casts `price` to float, then writes two derived tables: one enriched, one partitioned by country.
3. **Query** — DuckDB reads directly from the Iceberg tables on S3. Nothing is copied locally.

---

## Quickstart

```bash
make install     # install dependencies
make run         # generate CSV → ingest → transform
make query       # query the result with DuckDB
```

---

## Demo walkthrough

### 1. Run the full pipeline

```
$ make run

Generated 5,000 records → data/events.csv
Ingested → s3://iceberg/raw/events2/metadata/00009-....metadata.json
Enriched  → s3://iceberg/transformed/events_enriched/metadata/00001-....metadata.json
Partitioned → s3://iceberg/transformed/events_by_country/metadata/00001-....metadata.json
```

Every `make ingest` **appends** to the raw table — it never overwrites.
Every `make transform` **recreates** the enriched tables from the latest raw data.

---

### 2. See snapshot history (time travel)

```
$ make time-travel NAMESPACE=raw TABLE=events2

table location: s3://iceberg/raw/events2

  snapshot 4507200733701048832: 10 rows
  snapshot 1191308141860795509: 20 rows
  snapshot 1039873267403556773: 30 rows
  snapshot 6932781526553223944: 5,030 rows
  snapshot 5106723956069556564: 10,030 rows
  snapshot 343038567448800791:  15,030 rows
  snapshot 464977929981193173:  20,030 rows
  snapshot 435454061858668783:  25,030 rows
  snapshot 1159396667115235109: 30,030 rows
```

Each row is a **snapshot** — a point in time after a write. You can see the table grew from 10 rows all the way to 30,030 across 9 ingests. The data on S3 is never deleted, just a new snapshot is added on top.

---

### 3. Query a past snapshot

```
$ make query SQL="SELECT count(*) FROM iceberg_scan('s3://iceberg/raw/events2', snapshot_from_id=435454061858668783)"

 count_star()
        25030
```

This reads the table **as it was at snapshot `435...`** — only 25,030 rows, even though the table now has 30,030. The latest 5,000 rows don't exist yet from this snapshot's perspective.

This is **time travel** — go back to any point in history without keeping separate copies of the data.

---

## Make targets

| Command | What it does |
|---------|-------------|
| `make generate` | Generate a fake CSV with Faker |
| `make ingest` | Append CSV → Iceberg raw table |
| `make transform` | Rebuild enriched + partitioned tables |
| `make schema-evolve` | Add new columns to a live table |
| `make time-travel` | List all snapshots with row counts |
| `make query SQL="..."` | Run SQL via DuckDB against Iceberg |
| `make run` | generate + ingest + transform |
| `make clean` | Remove local CSV and DuckDB files |

---

## Key Iceberg concepts learned

| Concept | What it means |
|---------|--------------|
| **Catalog** | Metadata store (SQLite here) that tracks all tables and their S3 locations |
| **Namespace** | Logical grouping of tables — like a database schema |
| **Snapshot** | Immutable point-in-time state of a table after every write |
| **Time travel** | Query a table as it was at any past snapshot |
| **Partitioning** | Split data files by column value (e.g. country) so queries skip irrelevant files |
| **Schema evolution** | Add/rename columns on a live table without rewriting existing data |
