# PhonePe Data Engineering Interview Questions

**Source:** LinkedIn Post (PhonePe DE Interview Prep)
**Role Level:** 4+ Years Experience | CTC ~32 LPA

---

# Interview Questions

---

## Question 1
How would you design a scalable data pipeline that ingests millions of credit records daily with minimal latency?

## Answer

The core challenge here is balancing throughput (millions of records/day) with latency (near real-time processing). These two goals often pull in opposite directions, so your design needs to be explicit about which you're optimizing for.

**Practical Approach:**

Split the pipeline into two tracks:

1. **Fast path (streaming)** — For low-latency use cases like fraud detection or credit limit alerts. Use Kafka to ingest events as they happen, process with Flink or Spark Structured Streaming, and write to a low-latency store (Cassandra, Redis, or HBase).

2. **Slow path (batch)** — For analytical use cases like monthly credit reports or risk scoring. Ingest raw records into a data lake (S3/ADLS), run Spark jobs on a schedule, and write to a warehouse (Snowflake/BigQuery).

This is commonly called the **Lambda Architecture** (though Kappa Architecture simplifies it by using only streaming — more on this in Trade-offs).

## Architecture Explanation
```
Credit Source Systems (Loan Apps, Payment Events)
         │
         ▼
   ┌─────────────┐
   │   Kafka     │  ← Event ingestion layer (partitioned by customer_id)
   └──────┬──────┘
          │
    ┌─────┴──────┐
    │            │
    ▼            ▼
┌────────┐  ┌──────────────┐
│ Flink  │  │  Spark Batch │
│(Stream)│  │  (Scheduled) │
└───┬────┘  └──────┬───────┘
    │              │
    ▼              ▼
┌────────┐   ┌───────────┐
│ Redis/ │   │  Delta    │
│Cassand.│   │  Lake /   │
│(Fast   │   │  Snowflake│
│ reads) │   │(Analytics)│
└────────┘   └───────────┘
```

**Key design decisions:**
- Partition Kafka topics by `customer_id` to preserve order per customer
- Use idempotent writes to avoid duplicate records during retries
- Apply backpressure mechanisms so downstream systems aren't overwhelmed

## Trade-offs

| Approach | When it works | When it doesn't |
|---|---|---|
| Lambda (batch + stream) | When you need both real-time alerts AND historical analytics | Operational overhead — two codebases to maintain |
| Kappa (stream only) | Simpler ops, single codebase | Reprocessing large historical data is expensive |
| Batch only | Simple pipelines, high latency acceptable | Doesn't work for real-time fraud/alert use cases |

## Follow-up Questions
- How would you handle a Kafka consumer that falls behind during a traffic spike?
- How do you guarantee exactly-once delivery in your pipeline?
- How would you monitor pipeline health and SLA breaches?

---

## Question 2
How do you handle schema evolution in a data lake environment?

## Answer

Schema evolution means your data format changes over time — a new field is added, a column is renamed, or a data type changes. In a data lake, you don't control the source schema, so you need a strategy to handle this gracefully without breaking downstream pipelines.

**Three levels of schema evolution:**

1. **Additive changes** (new column added) — Easiest. Use schema-on-read formats like Parquet/ORC with Delta Lake or Apache Iceberg. New columns default to `null` for old records.

2. **Destructive changes** (column removed or renamed) — Harder. You need version tracking and backward-compatible reads.

3. **Type changes** (int → string) — Most dangerous. Requires explicit migration and testing.

**Practical tools:**
- **Delta Lake / Apache Iceberg** — Native schema evolution support with `MERGE SCHEMA` option
- **AWS Glue Schema Registry / Confluent Schema Registry** — Enforces schema contracts at the Kafka level using Avro or Protobuf
- **dbt** — Tracks column-level changes in transformation layer
```
Producer (new schema)
      │
      ▼
Schema Registry ──── validates against registered schema
      │
      ▼
Kafka Topic
      │
      ▼
Delta Lake Table ──── mergeSchema = true
      │
      ▼
Downstream Jobs (use schema metadata to handle nulls/defaults)
```

## Trade-offs

- **Schema Registry (strict)** — Prevents bad data from entering the lake. But slows down producers who need to register changes first.
- **Schema-on-read (flexible)** — Easy to ingest anything. But schema issues surface late, often in production analytics queries.
- **Best practice:** Validate at ingestion (Schema Registry) AND at consumption (Delta Lake schema enforcement)

## Follow-up Questions
- How would you handle a breaking schema change from an upstream team that didn't notify you?
- What's the difference between schema evolution in Avro vs Parquet?
- How does Iceberg's hidden partitioning help with schema evolution?

---

## Question 3
What is the difference between batch processing and stream processing? When would you prefer one over the other?

## Answer

**Batch processing** — Process a large chunk of data at a scheduled interval (hourly, daily). You wait for data to accumulate, then run a job over it.

**Stream processing** — Process each event (or micro-batch) as it arrives, continuously.

**Simple analogy:** Batch is like doing laundry once a week. Streaming is like washing each item the moment it gets dirty.

| Factor | Batch | Streaming |
|---|---|---|
| Latency | Minutes to hours | Milliseconds to seconds |
| Complexity | Simpler to build | Harder (state management, fault tolerance) |
| Cost | Lower (optimized compute windows) | Higher (always-on infrastructure) |
| Use cases | Reporting, ETL, ML training | Fraud detection, alerts, live dashboards |
| Tools | Spark, Hive, dbt | Flink, Spark Structured Streaming, Kafka Streams |

**When to choose batch:** Monthly billing reports, end-of-day credit summaries, model retraining pipelines.

**When to choose streaming:** Real-time credit utilization alerts, fraud detection on payment events, live customer risk scoring.

## Follow-up Questions
- What is micro-batching and how does Spark Structured Streaming use it?
- How does watermarking help with late-arriving data in stream processing?
- What's the difference between event time and processing time?

---

## Question 4
How would you design a data model for storing customer credit histories that supports both fast lookups and analytical queries?

## Answer

The challenge is that fast lookups and analytical queries have opposing needs:
- **Fast lookups** need data organized by customer (row-oriented, indexed)
- **Analytical queries** need data organized by columns across many customers (columnar, partitioned)

**Solution: Separate operational and analytical stores (CQRS-like pattern)**
```
┌─────────────────────────────┐
│      Credit Events          │  ← Raw source of truth
└────────────┬────────────────┘
             │
    ┌─────────┴──────────┐
    │                    │
    ▼                    ▼
┌──────────┐      ┌─────────────┐
│Cassandra │      │  Snowflake  │
│or DynamoDB      │  (Delta     │
│          │      │   Lake)     │
│ Key:     │      │             │
│customer_ │      │ Partitioned │
│id        │      │ by month,   │
│          │      │ customer    │
│Fast point│      │ segment     │
│lookups   │      │             │
│(ms)      │      │Analytics    │
└──────────┘      └─────────────┘
```

**Analytical model (Star Schema):**
```
        fact_credit_transactions
        ┌─────────────────────┐
        │ transaction_id (PK) │
        │ customer_id (FK)    │──── dim_customer
        │ account_id (FK)     │──── dim_account
        │ date_id (FK)        │──── dim_date
        │ credit_used         │
        │ credit_limit        │
        │ utilization_pct     │
        └─────────────────────┘
```

**Partition strategy for analytics:** Partition by `year_month` and cluster by `customer_segment` so range scans on time are efficient.

## Follow-up Questions
- How would you handle slowly changing dimensions (SCD) for customer attributes?
- What's the difference between a star schema and a snowflake schema?
- How would you model credit limit changes over time?

---

## Question 5
Given a table of credit transactions, write a SQL query to find customers whose credit utilization increased by more than 30% month over month.

## Answer

**Assume table structure:**
```sql
credit_transactions (
    customer_id     STRING,
    transaction_date DATE,
    credit_used      DECIMAL,
    credit_limit     DECIMAL
)
```

**Query:**
```sql
WITH monthly_utilization AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', transaction_date)       AS month,
        SUM(credit_used) / NULLIF(MAX(credit_limit), 0) * 100 AS utilization_pct
    FROM credit_transactions
    GROUP BY 1, 2
),
month_over_month AS (
    SELECT
        customer_id,
        month,
        utilization_pct,
        LAG(utilization_pct) OVER (
            PARTITION BY customer_id
            ORDER BY month
        ) AS prev_month_utilization_pct
    FROM monthly_utilization
)
SELECT
    customer_id,
    month,
    ROUND(prev_month_utilization_pct, 2)  AS prev_month_pct,
    ROUND(utilization_pct, 2)             AS curr_month_pct,
    ROUND(utilization_pct - prev_month_utilization_pct, 2) AS change_pct
FROM month_over_month
WHERE utilization_pct - prev_month_utilization_pct > 30
ORDER BY change_pct DESC;
```

**Key techniques used:**
- `DATE_TRUNC` — Normalize dates to month level
- `LAG()` window function — Access previous month's value without a self-join
- `NULLIF` — Prevent division by zero on credit_limit
- Filter in outer query (not WHERE on window function directly — window functions are evaluated before WHERE)

## Follow-up Questions
- How would you modify this query to look at the last 3-month trend instead?
- How would you handle customers who have no transactions in a given month?
- What index would you add to optimize this query?

---

## Question 6
What are database indexes and how do they help query performance? Can they slow down operations?

## Answer

An index is a separate data structure (usually a B-tree or hash) that stores a sorted copy of one or more columns alongside pointers to the actual rows. Think of it like the index at the back of a textbook — instead of reading every page to find "Kafka," you go directly to the page number.

**How they help:**
- Dramatically reduce rows scanned during `WHERE`, `JOIN`, and `ORDER BY`
- A query on `customer_id` without an index scans every row (O(n))
- With a B-tree index on `customer_id`, it's O(log n)

**How they can hurt:**

| Operation | Impact |
|---|---|
| SELECT | Faster (fewer rows scanned) |
| INSERT | Slower (index must be updated for every new row) |
| UPDATE | Slower (index updated if indexed column changes) |
| DELETE | Slower (index entry must be removed) |
| Storage | Indexes consume additional disk space |

**Practical rule:** Add indexes on columns used frequently in `WHERE`, `JOIN ON`, or `ORDER BY`. Avoid over-indexing on tables with heavy write workloads (e.g., a Kafka consumer sink table).

**Types relevant to Data Engineering:**
- **B-tree index** — General purpose, good for range queries
- **Bitmap index** — Good for low-cardinality columns (e.g., `status = 'active'`) — common in data warehouses
- **Clustered index** — Physically sorts the table by the indexed column (SQL Server, MySQL InnoDB)
- **Columnar storage** (Snowflake/BigQuery) — Effectively indexes by column, making analytical aggregations fast without explicit indexes

## Follow-up Questions
- What is a composite index and when would you use one?
- What is index cardinality and why does it matter?
- How does Snowflake handle query optimization without traditional indexes?

---

## Question 7
How would you optimize a slow-running SQL query on a 1 TB table?

## Answer

Optimization is a diagnostic process. Don't guess — always start with `EXPLAIN` / `EXPLAIN ANALYZE` to see what the query planner is actually doing.

**Systematic approach:**

**Step 1 — Understand the query plan**
```sql
EXPLAIN ANALYZE
SELECT ...
FROM big_table
WHERE ...
```
Look for: Full Table Scans, Hash Joins on large tables, missing indexes, high row estimates vs actual rows.

**Step 2 — Reduce data scanned early**
- Push filters as early as possible in the query
- Use partitioning — if the table is partitioned by `date`, filter on `date` first so only relevant partitions are scanned (partition pruning)
- Avoid `SELECT *` — only pull columns you need (columnar formats like Parquet benefit heavily from this)

**Step 3 — Fix join order and type**
- Join smaller tables first to reduce intermediate result size
- Avoid joining on non-indexed columns
- In distributed systems (Spark/BigQuery), use broadcast joins for small lookup tables

**Step 4 — Rewrite inefficient patterns**
```sql
-- Bad: function on indexed column defeats the index
WHERE YEAR(transaction_date) = 2024

-- Good: range condition allows index/partition use
WHERE transaction_date BETWEEN '2024-01-01' AND '2024-12-31'
```

**Step 5 — Materialization**
- Pre-aggregate frequently queried summaries into materialized views or summary tables
- Use clustering keys in Snowflake on frequently filtered columns

## Follow-up Questions
- What is query predicate pushdown and how does Spark use it?
- When would you use a materialized view vs a regular view?
- How does Snowflake's automatic clustering work?

---

## Question 8
How have you used Spark for ETL pipelines? What are common performance bottlenecks and how do you fix them?

## Answer

Spark processes data in parallel across a cluster by splitting data into partitions. Each partition is processed independently on a worker node. Understanding this model is the key to diagnosing all performance issues.

**Common ETL pattern in Spark:**
```
Raw Data (S3/ADLS)
    │
    ▼
Read (DataFrame)  ──── schema inference, partition discovery
    │
    ▼
Transform          ──── filter, join, aggregate, deduplicate
    │
    ▼
Write (Delta Lake) ──── partitioned output, Z-ordering
```

**Top bottlenecks and fixes:**

| Bottleneck | Symptom | Fix |
|---|---|---|
| Data skew | One task takes 10x longer than others | Salting keys, repartition by skewed column |
| Shuffle (wide transforms) | Many GBs shuffled across network | Reduce shuffles, use `broadcast()` for small tables |
| Too many small files | Thousands of tiny Parquet files, slow reads | Coalesce before write, Delta Lake `OPTIMIZE` |
| Too few partitions | Low parallelism, executors idle | Repartition to match cluster cores (rule: 2-4x vCores) |
| UDFs in Python | Serialization overhead vs native Spark SQL | Replace Python UDFs with Spark built-in functions |
| Reading too much data | Full table scan | Apply partition filters, use columnar pruning |

**Practical example — handling data skew:**
```python
# Skewed join on customer_id where VIP customers dominate
# Salt the key to distribute load
from pyspark.sql.functions import concat, col, lit, rand, floor

df = df.withColumn("salted_key",
    concat(col("customer_id"), lit("_"), (floor(rand() * 10)).cast("string"))
)
```

## Follow-up Questions
- What is the difference between `repartition()` and `coalesce()` in Spark?
- How does Spark's Catalyst optimizer work?
- What is AQE (Adaptive Query Execution) in Spark 3.x?

---

## Question 9
What is the difference between a data warehouse (Snowflake, Redshift, BigQuery) and a data lake (S3, ADLS)?

## Answer

| Factor | Data Warehouse | Data Lake |
|---|---|---|
| Data format | Structured (tables, schemas) | Any format (JSON, CSV, Parquet, images, logs) |
| Schema | Schema-on-write (defined before load) | Schema-on-read (defined at query time) |
| Query speed | Fast (optimized storage, indexes, caching) | Slower for ad-hoc (depends on file format and engine) |
| Cost | Higher per TB stored | Lower per TB stored |
| Users | BI analysts, SQL users | Data engineers, data scientists |
| Examples | Snowflake, Redshift, BigQuery | S3 + Athena, ADLS + Databricks, GCS + BigQuery |

**Modern reality — the Data Lakehouse:**

The industry has largely moved toward a **Lakehouse** pattern, which combines the storage economics of a data lake with the performance and ACID guarantees of a warehouse.
```
Raw Zone (S3/ADLS)         ← All raw data lands here
    │
    ▼
Curated Zone (Delta Lake)  ← Cleaned, structured tables with ACID
    │
    ▼
Serving Zone (Snowflake)   ← Aggregated, BI-ready tables
```

Technologies like **Delta Lake**, **Apache Iceberg**, and **Apache Hudi** enable ACID transactions, time travel, and schema enforcement directly on cloud object storage — blurring the line between lake and warehouse.

## Follow-up Questions
- What is the difference between Delta Lake, Apache Iceberg, and Apache Hudi?
- When would you still choose a pure data warehouse over a lakehouse?
- What is time travel in Delta Lake and when would you use it?

---

## Question 10
How do you ensure data quality when building pipelines?

## Answer

Data quality is not a one-time check — it's a continuous process built into every stage of the pipeline.

**Framework: Validate at 3 layers**
```
┌─────────────────────────────────────────────────────┐
│ Layer 1: Source Validation (before ingestion)        │
│  - Schema conformance (correct columns/types)        │
│  - Null checks on mandatory fields                   │
│  - Row count checks (expected range)                 │
└─────────────────────────┬───────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────┐
│ Layer 2: Pipeline Validation (during transformation) │
│  - Deduplication (customer_id + date uniqueness)     │
│  - Referential integrity (customer exists in dim)    │
│  - Business rule validation (utilization 0-100%)     │
│  - Statistical drift detection (sudden volume drop)  │
└─────────────────────────┬───────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────┐
│ Layer 3: Output Validation (after write)             │
│  - Row count reconciliation (source vs target)       │
│  - Aggregate checks (SUM of credit should match)     │
│  - Freshness checks (data arrived within SLA window) │
└─────────────────────────────────────────────────────┘
```

**Tools commonly used:**
- **Great Expectations** / **dbt tests** — Declarative data quality rules
- **Apache Griffin** — Open-source DQ framework for big data
- **Monte Carlo / Bigeye** — Automated anomaly detection (data observability)
- **dbt** — `not_null`, `unique`, `accepted_values`, `relationships` tests natively

**Handling failures:**
- Don't silently drop bad records — quarantine them in a dead-letter table for investigation
- Alert on quality breaches before they reach downstream consumers
- Document SLAs: "Credit data must arrive by 3 AM with <0.1% null rate on customer_id"

## Follow-up Questions
- How would you implement idempotent pipeline runs to prevent duplicate data?
- What is data observability and how is it different from data quality checks?
- How would you handle late-arriving data that misses your quality checks window?

---

# Concepts to Study

**Data Pipeline Design**
A pipeline is simply a series of steps that move data from one place to another, transforming it along the way. In production, reliability matters more than cleverness — pipelines should be idempotent (safe to re-run), observable (you can see what's happening), and recoverable (failures don't corrupt data).

**Schema Evolution**
Your upstream data will always change without warning. Build pipelines that can handle new columns gracefully (additive changes) and have explicit alerting for breaking changes (type changes, column removals).

**Window Functions in SQL**
`LAG`, `LEAD`, `RANK`, `ROW_NUMBER`, `SUM OVER` — these allow you to compute values across related rows without collapsing the result set. Critical for time-series analysis on financial data.

**Spark Execution Model**
Spark builds a DAG (Directed Acyclic Graph) of operations and only executes when an action is triggered (`.write()`, `.collect()`). Wide transformations (joins, groupBy) cause shuffles — moving data across the network — which are the primary source of performance problems.

**Data Quality vs Data Observability**
Quality = "Does this specific column meet this rule right now?"
Observability = "Is the overall health of my data pipelines normal over time?" (volume trends, freshness, schema drift)

---

# Real-world Technology Mapping

| Technology | Role in Production |
|---|---|
| **Kafka** | Ingestion layer — durable, ordered event stream. PhonePe would use this to capture payment/credit events in real-time |
| **Spark** | Large-scale batch ETL — reprocessing months of credit history, feature engineering for ML models |
| **Flink** | Real-time stream processing — sub-second fraud detection, live credit utilization scoring |
| **Airflow** | Orchestration — scheduling batch Spark jobs, managing pipeline dependencies, alerting on failures |
| **Delta Lake** | ACID-compliant lakehouse storage — storing credit history with time travel for auditing and point-in-time recovery |
| **Snowflake** | Analytical serving layer — BI dashboards, ad-hoc SQL queries by analysts, credit risk reporting |
| **Azure (ADLS)** | Raw storage — landing zone for all raw credit transaction files before processing |

---

# Key Takeaways

1. **Lambda vs Kappa** — Know both architectures. Lambda separates batch and stream; Kappa unifies them. Be ready to argue trade-offs.

2. **Schema evolution** — Always use Delta Lake or Iceberg in a lakehouse. Always use a Schema Registry for Kafka. Never assume the source schema is stable.

3. **SQL window functions** — `LAG()`, `LEAD()`, `ROW_NUMBER()` are mandatory skills for financial data analysis questions. Practice them daily.

4. **Spark performance** — The answer to almost every Spark performance question comes down to: partitioning, shuffles, and skew. Know all three cold.

5. **Data quality is a pipeline feature, not an afterthought** — Mention Great Expectations, dbt tests, and dead-letter queues in every pipeline design answer.

6. **Lakehouse is the current industry standard** — Warehouse vs Lake is a false choice in 2025. The right answer is almost always "we use a lakehouse with Delta Lake or Iceberg on top of cloud object storage."