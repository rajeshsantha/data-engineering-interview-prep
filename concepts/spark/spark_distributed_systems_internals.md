# Apache Spark & Distributed Systems Internals — Interview Prep

**Source:** https://www.linkedin.com/posts/dibya-ranjan-rath-b4b922100_dataengineering-apachespark-distributedsystems-activity-7431177665081987072-sAwE/
**Tags:** #dataengineering #apachespark #distributedsystems
**Suggested Repo Path:** `concepts/spark/spark_distributed_systems_internals.md`

---

# Interview Questions

---

## Question 1
### How does Apache Spark's execution model work internally — from the moment you call an action to when results are returned?

## Answer

When you write a Spark job and trigger an **action** (like `.count()` or `.write()`), here's what actually happens under the hood:

1. **DAG Creation** — Spark's Catalyst optimizer converts your DataFrame/RDD operations into a logical plan, then an optimized physical plan, represented as a Directed Acyclic Graph (DAG) of stages.

2. **Stage Splitting** — The DAG is cut into **stages** at shuffle boundaries. Anything that requires data to move between partitions (like `groupBy`, `join`, `repartition`) creates a new stage.

3. **Task Scheduling** — Each stage is broken into **tasks**, one per partition. The Driver sends these tasks to the Cluster Manager (YARN, Kubernetes, Mesos, or Standalone).

4. **Execution on Executors** — Worker nodes (Executors) run the tasks in parallel. Each executor is a JVM process with allocated CPU cores and memory.

5. **Shuffle & Result Collection** — If stages depend on each other, shuffle data is written to disk (or memory) and pulled by downstream tasks. Final results are either returned to the Driver or written to storage.

```
User Code (Action Triggered)
        │
        ▼
┌──────────────────────┐
│   Driver Program     │
│  - DAG Scheduler     │  ← Builds DAG, splits into Stages
│  - Task Scheduler    │  ← Sends Tasks to Executors
└──────────┬───────────┘
           │  (via Cluster Manager)
    ┌──────┴──────┐
    ▼             ▼
┌────────┐   ┌────────┐
│Executor│   │Executor│   ← Each runs tasks on partitions
│ Node 1 │   │ Node 2 │
└────────┘   └────────┘
    │             │
    └──────┬──────┘
           ▼
     Shuffle / Output
```

**Key insight:** Spark is *lazy*. Transformations (`map`, `filter`, `join`) are not executed immediately — they just build the plan. Only **actions** trigger actual computation. This is why Spark can optimize the entire pipeline before running a single task.

---

## Architecture Explanation

In a production cluster (e.g., Spark on YARN):

- The **Driver** runs on an edge node or master node. It's responsible for the entire job lifecycle.
- **Executors** are launched on worker nodes. They hold data in memory (as partitions) and compute tasks.
- The **Cluster Manager** (YARN ResourceManager / Kubernetes API server) allocates containers for executors.
- **SparkContext** (or SparkSession) is the entry point that connects the driver to the cluster.

```
┌─────────────────────────────────────────┐
│            YARN / Kubernetes            │
│  ┌────────────────┐  ┌───────────────┐  │
│  │  Driver (JVM)  │  │  Executor 1   │  │
│  │  SparkSession  │  │  Task Task    │  │
│  │  DAG Scheduler │  │  Partition 0  │  │
│  │  Task Scheduler│  └───────────────┘  │
│  └────────────────┘  ┌───────────────┐  │
│                      │  Executor 2   │  │
│                      │  Task Task    │  │
│                      │  Partition 1  │  │
│                      └───────────────┘  │
└─────────────────────────────────────────┘
         │                   │
         └─────── HDFS / S3 / GCS (Storage) ──────┘
```

---

## Trade-offs

| Approach | Works Well When | Avoid When |
|---|---|---|
| In-memory processing (Spark) | Dataset fits in executor memory; iterative ML workloads | Data exceeds available cluster memory → spills to disk, slowing jobs |
| Larger partitions | Fewer tasks, less scheduling overhead | Risk of OOM errors if partitions too large |
| Smaller partitions | Better parallelism | Too many tiny tasks → scheduling overhead kills performance |
| Persist/Cache RDD | Same dataset reused across multiple actions | Storage is tight or dataset is large and rarely reused |

---

## Follow-up Questions

- What happens when an executor fails mid-job? How does Spark recover?
- How does speculative execution work in Spark, and when would you enable it?
- Explain the difference between `cache()` and `persist()` in Spark.
- How does Spark decide how many partitions to create for a given stage?

---

## Question 2
### What is data shuffling in Spark, and why is it expensive?

## Answer

A **shuffle** is when Spark needs to redistribute data across partitions — typically triggered by operations like `groupByKey`, `reduceByKey`, `join`, or `repartition`.

Here's why it's expensive:

1. **Disk I/O** — Shuffle data is written to disk by the mapper tasks (shuffle write), then read back by reducer tasks (shuffle read).
2. **Network I/O** — Data travels across the network from one executor to another.
3. **Serialization/Deserialization** — Data must be serialized before sending and deserialized after receiving.
4. **GC Pressure** — Large shuffle operations put pressure on JVM garbage collection.

```
Before Shuffle (each executor has mixed keys):

Executor 1:  [A:1, B:2, A:3]
Executor 2:  [B:4, C:5, A:6]

After Shuffle (same keys grouped together):

Executor 1:  [A:1, A:3, A:6]   ← All A's
Executor 2:  [B:2, B:4]        ← All B's
Executor 3:  [C:5]             ← All C's
```

**Practical tip:** Prefer `reduceByKey` over `groupByKey`. With `groupByKey`, all values are shuffled before aggregation. With `reduceByKey`, values are partially aggregated on each partition first (like a local combiner in MapReduce), dramatically reducing shuffle volume.

---

## Architecture Explanation

Spark uses a **sort-based shuffle** by default (since Spark 1.2). Here's the flow:

```
Map Phase (on Executor):
  ┌─────────────┐
  │ Partition 0 │ → Sort by partition key → Write to shuffle files
  └─────────────┘

Reduce Phase (on different Executor):
  ┌──────────────────┐
  │ Fetch shuffle    │ → Pull relevant partitions → Deserialize → Process
  │ files from map   │
  │ outputs (HTTP)   │
  └──────────────────┘
```

External Shuffle Service (when enabled) allows executors to fetch shuffle files even if the producing executor has been killed/recycled — important for long-running jobs on dynamic allocation.

---

## Trade-offs

- **Avoid wide transformations** when a narrow alternative exists (e.g., `mapPartitions` vs `groupByKey`).
- **Broadcast joins** eliminate shuffle for small tables: Spark sends the small table to every executor, avoiding the need to shuffle the large table.
- **Salting** is used to fix skewed shuffles — add a random prefix to keys so skewed keys get distributed across multiple partitions.

---

## Follow-up Questions

- How would you diagnose a shuffle-heavy job in Spark UI?
- What is shuffle spill to disk, and how do you reduce it?
- Explain how broadcast join works and when you should use it.
- How does Spark handle skewed joins in production?

---

## Question 3
### What is the difference between narrow and wide transformations in Spark?

## Answer

This is one of the most fundamental concepts in Spark's execution model.

**Narrow Transformations** — Each input partition contributes to exactly one output partition. No data movement between nodes. Fast, pipelined within a stage.

Examples: `map`, `filter`, `flatMap`, `mapPartitions`, `union`

**Wide Transformations** — Input partitions may contribute to multiple output partitions. Requires a shuffle. Spark creates a new stage at each wide transformation.

Examples: `groupByKey`, `reduceByKey`, `join`, `distinct`, `repartition`, `sortBy`

```
Narrow (no shuffle):                Wide (shuffle needed):

Partition 1 → Partition 1           Partition 1 ──┐
Partition 2 → Partition 2           Partition 2 ──┼──→ Partition A
Partition 3 → Partition 3           Partition 3 ──┘
                                    (data redistributed)
```

**Why it matters for performance:**
- Narrow transformations can be **pipelined** — Spark combines multiple narrow steps into a single task.
- Wide transformations force a **stage boundary**, meaning Spark must materialize intermediate data and schedule a new round of tasks.
- Minimizing wide transformations = fewer shuffles = faster jobs.

---

## Follow-up Questions

- Can you give an example of rewriting a wide transformation to use narrow ones?
- How does Spark's Catalyst optimizer handle wide vs narrow transformations in query planning?
- What is stage-level scheduling and how does it relate to wide transformations?

---

## Question 4
### How does fault tolerance work in Apache Spark?

## Answer

Spark achieves fault tolerance through **lineage** (for RDDs) and **checkpointing** (for streaming or long lineage chains).

**RDD Lineage:**
Every RDD knows how it was built — its *lineage* is the sequence of transformations from the original data source. If a partition is lost (executor dies), Spark simply recomputes that partition by replaying the lineage from the last stable point.

```
Source Data (S3/HDFS)
      │
      ▼
   RDD A (map)
      │
      ▼
   RDD B (filter)
      │
      ▼
   RDD C (reduceByKey)  ← If a partition here is lost,
                           Spark recomputes from Source → A → B → C
```

**When lineage gets too long:**
For iterative jobs (like ML training loops) or long streaming pipelines, recomputing from the very beginning is expensive. **Checkpointing** saves the RDD/DataFrame to reliable storage (HDFS, S3) and cuts the lineage at that point. Recovery then starts from the checkpoint, not from the original source.

**For Structured Streaming:**
Spark uses a **Write-Ahead Log (WAL)** and **checkpoint directories** to store offsets and intermediate state, ensuring exactly-once or at-least-once processing guarantees.

---

## Trade-offs

| Strategy | Pros | Cons |
|---|---|---|
| Lineage recomputation | No storage overhead; works for short DAGs | Slow for long/complex lineages |
| Checkpointing | Fast recovery; breaks long lineage | Adds write latency; needs reliable storage |
| Replication (persist with MEMORY_AND_DISK_2) | Immediate failover without recompute | 2x memory/disk cost |

---

## Follow-up Questions

- How do you enable checkpointing in Spark Streaming?
- What's the difference between RDD checkpointing and Structured Streaming checkpointing?
- How does Spark handle speculative task execution to deal with slow nodes (stragglers)?
- What happens to in-flight shuffle data when an executor fails?

---

# Concepts to Study

**DAG (Directed Acyclic Graph):**
A way of representing the sequence of computations. "Directed" means each step flows in one direction (source → result). "Acyclic" means no loops. Spark uses this to optimize and schedule your job before running anything.

**Lazy Evaluation:**
Spark does not execute transformations the moment you write them. It builds a plan and waits for an action. This allows Spark to combine multiple steps, push down filters, and eliminate unnecessary operations — all before touching any data.

**Partitioning:**
Your data is split into chunks called partitions. Each partition is processed independently on a different CPU core or node. More partitions = more parallelism, but also more scheduling overhead. Typically, aim for partition sizes of 128–256 MB.

**Catalyst Optimizer:**
Spark's built-in query optimizer for DataFrame/Dataset API. It applies rule-based and cost-based optimizations: predicate pushdown, projection pruning, join reordering, etc. This is why DataFrame code is often faster than hand-written RDD code.

**Tungsten Execution Engine:**
Spark's physical execution layer that works below the JVM abstraction. It uses off-heap memory, code generation, and CPU-cache-aware data structures to squeeze maximum performance out of hardware.

**Executor Memory Model:**
```
┌────────────────────────────────────────────┐
│              Executor JVM Heap              │
│                                            │
│  ┌─────────────────┐  ┌─────────────────┐  │
│  │  Execution Mem  │  │  Storage Memory │  │
│  │  (shuffles,     │  │  (cached RDDs,  │  │
│  │   sort buffers) │  │   broadcasts)   │  │
│  └─────────────────┘  └─────────────────┘  │
│          Unified Memory (60% of heap)       │
│                                            │
│  ┌──────────────────────────────────────┐  │
│  │  User Memory (UDFs, data structures) │  │
│  └──────────────────────────────────────┘  │
└────────────────────────────────────────────┘
```
The two pools (execution + storage) share space — when one needs more, it borrows from the other up to configured limits.

---

# Real-world Technology Mapping

**Apache Spark (Batch Processing):**
Used at companies like Netflix, Uber, and LinkedIn for ETL pipelines processing petabytes of event data daily. Jobs are typically submitted via Apache Livy, Databricks Jobs API, or EMR Step Functions.

**Apache Spark Structured Streaming:**
Used as a near-real-time processing layer — e.g., reading from Kafka, applying windowed aggregations, and writing to Delta Lake or Cassandra. Latency in the seconds range (micro-batch) or milliseconds (continuous mode, experimental).

**Delta Lake (Databricks / OSS):**
Sits on top of Spark. Adds ACID transactions, schema enforcement, time travel, and efficient upserts (MERGE). In production, Delta tables replace raw Parquet as the standard storage format in the Lakehouse architecture.

**Apache Kafka → Spark Structured Streaming → Delta Lake:**
```
Kafka (raw events)
      │
      ▼
Spark Structured Streaming
  (windowed aggregations,
   deduplication, enrichment)
      │
      ▼
Delta Lake (Gold/Silver tables)
      │
      ▼
Snowflake / Databricks SQL
  (BI / Ad-hoc queries)
```

**Apache Airflow / Prefect / Dagster (Orchestration):**
Spark jobs are rarely run in isolation. Orchestrators schedule them, handle retries, manage dependencies (e.g., "only run the aggregation after the raw ingestion completes"), and send alerts on failure.

**Azure Databricks / AWS EMR / GCP Dataproc:**
Managed Spark services. They handle cluster provisioning, auto-scaling, and job monitoring. Databricks adds the Delta Engine (optimized Spark runtime), Unity Catalog, and collaborative notebooks on top.

**Snowflake:**
When data volumes are query-time (not transform-time), Snowflake is used instead of Spark. Snowflake excels at concurrent ad-hoc SQL queries, while Spark excels at heavy batch transformations and ML workloads.

---

# Key Takeaways

1. **Spark is lazy by design.** Transformations build a plan; actions execute it. Interviewers love asking "when does Spark actually run the computation?"

2. **Shuffles are the enemy of performance.** The fastest Spark optimization is often reducing or eliminating shuffles — via broadcast joins, pre-partitioning, or aggregating before shuffling.

3. **Narrow vs Wide is a stage boundary concept.** Every wide transformation = new stage = shuffle = disk + network I/O.

4. **Fault tolerance comes from lineage, not replication.** Spark doesn't replicate data by default (unlike HDFS). It recomputes lost partitions using the recorded transformation graph.

5. **Partition count matters enormously.** Too few → underutilized cores. Too many → scheduling overhead. Rule of thumb: 2–4x number of CPU cores in your cluster, with each partition ~128–256 MB.

6. **Know the memory model.** Interviewers for senior roles often ask about Spark OOM errors, memory tuning (`spark.memory.fraction`, `spark.memory.storageFraction`), and when to use off-heap memory.

7. **DataFrame API >> RDD API.** For most workloads, use DataFrames. Catalyst + Tungsten give you optimizations you'd never write by hand. Reserve RDD API for edge cases where you need low-level control.

8. **In a Lakehouse setup**, Spark is the transformation engine. Delta Lake is the storage format. Airflow/Prefect orchestrates the pipeline. Snowflake/Databricks SQL serves the queries. Know which layer does what.
