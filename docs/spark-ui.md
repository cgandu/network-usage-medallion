# Exploring the Spark UI locally

How to inspect jobs, stages, tasks, partitions and query plans **without the UI disappearing**
when a fast job finishes. Works in GitHub Codespaces and in WSL/Linux; written so a person or an
agent can follow it step by step.

> The Spark UI only exists locally. Databricks serverless has no Spark UI (use the query profile
> there); see `docs/architecture.md` §6.

## Why the UI "closes"

The UI at `http://localhost:4040` is served **by the running SparkSession** and disappears when
the session stops. The generator, the bronze job and pytest all stop their session within
seconds. Two ways around that:

| Method | When to use | Port |
|---|---|---|
| **A. Interactive session** | Explore live: run a query, look, change it, run again | 4040 |
| **B. Event log + History Server** | Inspect a job *after* it ran (generator, bronze job, tests) | 18080 |

Prerequisite for both: landing data exists (`data/` is gitignored, so a fresh machine has none).

```bash
uv sync
uv run python -m netusage.generator --profile tiny --days 7      # seconds
# optional, to explore Delta tables too:
uv run python -m netusage.jobs.bronze --start 2025-01-01 --end 2025-01-07
```

Use `--profile small` (1M events, about a minute) when you want stages with real work in them.

---

## A. Interactive session (live UI, no time limit)

1. Start Python **and keep it open**: the UI lives exactly as long as this prompt does.

   ```bash
   uv run python
   ```

2. Create the session with the project helper and run something:

   ```python
   from pyspark.sql import functions as F
   from netusage import config as C, io
   from netusage.session import local_spark

   spark = local_spark("explore")
   print(spark.sparkContext.uiWebUrl)  # usually http://...:4040

   logs = io.read_landing(spark, C.LOCAL_LANDING_PATH, io.EVENT_ENTITY)

   spark.sparkContext.setJobDescription("count traffic logs")  # names the job in the UI
   logs.count()

   spark.sparkContext.setJobDescription("users per batch_date")
   (
       logs.groupBy("batch_date")
       .agg(F.count("*").alias("n"), F.countDistinct("user_email").alias("users"))
       .orderBy("batch_date")
       .show()
   )

   logs.groupBy("batch_date").count().explain("formatted")  # plan in the terminal too
   ```

   Bronze Delta tables (after the bronze job):
   `io.read_delta(spark, io.table_target(C.LOCAL_BRONZE_PATH, "traffic_logs"))`.

3. **Open the UI**
   - **Codespaces:** bottom panel → **Ports** tab → port **4040** ("Spark UI", forwarded by
     `.devcontainer/devcontainer.json`) → globe icon "Open in Browser". It does not pop up on
     its own (`onAutoForward: silent`).
   - **WSL / Linux:** open `http://localhost:4040` in the browser.
   - If 4040 is taken (another session still running), Spark uses **4041**, 4042, … Check
     `uiWebUrl`, and forward that port manually in Codespaces (Ports → "Forward a Port").

4. Run as many queries as you like; every one appears in the UI. When done:
   `spark.stop()` then `exit()`. Exiting is what frees the CPU: a forgotten session keeps a JVM
   alive (`ps aux --sort=-%cpu | head` shows leftover `java` processes).

**Alternative for scripts:** end any script with `input("Spark UI is up, press Enter to stop")`
to hold the session open.

---

## B. Event logs + History Server (inspect after the fact)

Spark can record everything the UI shows into an **event log**; the History Server replays it
with the full UI (jobs, stages, tasks, SQL plans) for as long as you like. No code changes needed:
the settings go in through `PYSPARK_SUBMIT_ARGS`, which every local session in this project
picks up.

1. **Record.** Create the log folder once, then run any command with event logging on:

   ```bash
   mkdir -p data/spark-events                    # Spark fails if the folder doesn't exist
   export PYSPARK_SUBMIT_ARGS="--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file://$PWD/data/spark-events pyspark-shell"

   uv run python -m netusage.generator --profile tiny --days 7
   uv run pytest tests/test_silver.py            # tests can be recorded too
   ```

   The `export` lasts for this terminal only; open a new terminal (or `unset
   PYSPARK_SUBMIT_ARGS`) to stop recording. Each finished application becomes one file in
   `data/spark-events/`.

2. **Start the History Server** (in its own terminal; Ctrl+C stops it):

   ```bash
   SPARK_HOME=$(uv run python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
   SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file://$PWD/data/spark-events" \
     "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.history.HistoryServer
   ```

3. **Open** port **18080**: in Codespaces, Ports tab → "Forward a Port" → `18080` → open in
   browser; in WSL/Linux, `http://localhost:18080`. Click an application ID to get its full UI.
   New runs show up after a refresh (the server rescans the folder every ~10 s).

4. Clean up old runs with `rm data/spark-events/*` when the list gets long.

---

## What to look at

| Tab | What it shows | Look for |
|---|---|---|
| **SQL / DataFrame** | One entry per query: plan graph + "Details" (the physical plan) | Scan node metrics (files read, size, rows output), `Exchange` = shuffle, join type, `AQEShuffleRead coalesced`; initial vs final plan (AQE) |
| **Jobs** | One job per action (`count`, `show`, `write`, …) | Job descriptions you set, duration, number of stages |
| **Stages** | Each stage: tasks, shuffle read/write | Click a stage → task list and summary metrics (min/median/max duration, input size) → **skew** when max ≫ median |
| **Storage** | Cached DataFrames | Whether a `cache()` actually materialized, size per partition |
| **Environment** | Effective config | `spark.sql.shuffle.partitions` (16 in `local_spark`), AQE settings |
| **Executors** | Locally a single driver | Memory use, GC time |

Reading the plan text: read **bottom-up**; every `Exchange` is a stage boundary; `codegen id`
numbers line up with stages; `Statistics(sizeInBytes=8.0 EiB)` is just "unknown".

## Troubleshooting

- **Nothing on 4040:** the session already ended (method A keeps it open), or it is on 4041+.
- **`High codespace CPU` warning:** Spark uses every core it gets (2 on the default Codespace).
  Normal during a run; if it persists, look for leftover `java` processes.
- **`HADOOP_HOME and hadoop.home.dir are unset` on native Windows:** Spark can't write files
  there (generator, Delta, event logs). Method A works for read-only queries on existing data;
  for everything else use Codespaces or WSL.
- **History Server shows nothing:** check `ls data/spark-events`; the recording terminal must
  have had `PYSPARK_SUBMIT_ARGS` exported *before* the run started.
