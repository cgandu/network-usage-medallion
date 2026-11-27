# Parking lot

Interesting tangents, one line each. Revisit after the core pipeline is done.

- Spark-native transitive AD group expansion (iterative joins / GraphFrames) — upstream in the real system.
- Bronze on Databricks has `clusterByAuto: true` (automatic liquid clustering) and deletion vectors: see what they do to file layout over many daily re-runs (session 8).
- Track how users/apps/rules change over time (SCD2 from daily snapshots) instead of using the latest snapshot.
- (Partly addressed: `bronze.ingest_audit` now counts corrupt rows reliably on Databricks.) Validation on Databricks: CSV column pruning hides extra-field rows from `_corrupt_record`, and `validation.py` relies on `cache()` (which serverless may ignore), so malformed counts could be wrong there.
  Observed on Databricks (SMALL): validation's malformed counts were correct (476 / 1,007, same as local and bronze), so either serverless honours `cache()` or its CSV reader doesn't prune; cause not investigated.
  Fix if it ever bites: validate malformed rows after bronze, or force a full parse (a query that consumes every column, since `select(*cols).count()` is still pruned, or `spark.sql.csv.parser.columnPruning.enabled=false` if serverless allows it).
- Silver `traffic_events` dedup: copies of an `event_id` in the same file tie on every sort key (`is_corrupt`, `batch_date`, `source_file`), so Spark may keep either one. Harmless with generated data (one file per batch, identical copies); with real data add a per-row key, e.g. a hash of the whole row (CSV has no `_metadata.row_index`).
- Silver `traffic_events` keeps `user_email_raw` and `protocol_raw`, though normalizing only removes case and whitespace (nothing a join or metric needs is lost). Dropping them would save some storage and write I/O at FULL scale (100M rows); measure before deciding.
- `silver.py` holds both the `Column -> Column` expressions and the table transforms. When the snapshot tables (users, rules, groups, memberships, resolutions) make it hard to read, split it, e.g. the expressions in one module and one module per table group.
