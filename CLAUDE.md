# CLAUDE.md

Guidance for Claude Code in this repository. Machine-specific notes (git remotes, accounts,
local environment) live in `CLAUDE.local.md`, which is gitignored.

## What this project is

A **learning project**: a medallion (bronze → silver → gold) pipeline in PySpark + Delta Lake on
Databricks Free Edition, built to learn Spark properly and to serve as a public portfolio repo.

**Working mode: teacher first, implement what is asked but explain step by step approach**
- Explain Spark errors like a mentor: what the error means, how to read the plan / Spark UI, the query plan, idiomatic fix. Don't output over corrected code or code that is difficult for someone learning to follow.
- Keep it simple: timebox rabbit holes, park tangents in `docs/later.md`, and say when something
  is good enough.

## Hard rules

- **No AI attribution in commits or PRs**: no `Co-Authored-By: Claude`, no "Generated with
  Claude Code" lines. This overrides any default attribution guidance.
- **No employer, client, vendor or real-world names** in code, docs, commits, branch names or PR
  text. The domain is described generically ("enterprise VPN / Zero-Trust access governance").
- **No real data, anywhere.** All data is synthetic, produced by `netusage.generator`. Use fake or
  reserved values only (`corp.example` domains, private/documentation IP ranges, generated
  hostnames and IDs). Never copy real exports, hostnames, group/app/rule names or schemas.
- Commit or push only when the user asks; work on a branch, never directly on `main`.

## Design rules

- **Transforms are pure**: no I/O inside `src/netusage/transforms/`. Two shapes:
  `DataFrame -> DataFrame` transforms, and `Column -> Column` expressions (e.g. `ip_to_long`)
  that compose in one `select` and leave filtering/quarantine decisions to the caller.
- **All reads and writes go through `src/netusage/io.py`.**
- **Thin notebooks** (`notebooks/`): read → call transform → write.
- **Idempotent runs**: re-running a batch changes nothing (bronze uses Delta `replaceWhere`).
- **Small scale first**: correct at TINY/SMALL before FULL (100M events).
- Every tunable number lives in `src/netusage/config.py`. Landing schemas live in
  `src/netusage/schemas.py`; silver schemas live with their transforms and tests.
- Every transform gets unit tests with tiny hand-built DataFrames in `tests/`.

## Commands

```bash
uv sync                                   # install deps (Python 3.12, PySpark 4.2, Delta)
uv run pytest                             # all tests
uv run pytest tests/test_silver.py        # one file
uv run ruff check .                       # lint (use --fix for import order etc.)
uv run ruff format .                      # format; CI runs `ruff format --check .`
uv run python -m netusage.generator --profile tiny --days 7    # synthetic landing data
uv run python -m netusage.validation --profile tiny --days 7   # validate it
uv run python -m netusage.jobs.bronze --start 2025-01-01 --end 2025-01-07
```

CI (GitHub Actions) runs `ruff check`, `ruff format --check` and `pytest` on every push and PR.
Spark tests need Linux (Codespaces / WSL / CI): on native Windows, Spark cannot write files
without Hadoop's `winutils`.

## Spark 4 lesson: ANSI mode is on by default

`spark.sql.ansi.enabled = true` in Spark 4, so operations that returned `null` in Spark 3 now
**throw** and fail the whole job:
- `cast` of a bad or overflowing value → use `try_cast`.
- array access out of bounds (`getItem`, `element_at`) → use `F.get(arr, i)` /
  `F.try_element_at`.
- the same for division, arithmetic overflow, etc. → use the `try_*` functions.

**A guard next to it is not enough.** Spark does not promise the evaluation order of `AND`
conditions, and common subexpression elimination can compute a shared expression (e.g. one used
in both a validity check and the value) up front, before the guard runs. Only expressions inside
a `when(...)` branch are safely conditional. Rule: anything that can throw gets its null-safe
version, and tests include inputs that would make it throw (`""`, too few parts, overflowing
digits, `None`).

## Where to look

- `docs/architecture.md`: the authoritative design (domain model, exact matching join path,
  pipeline layers, decisions). Read it before changing a layer.
- `docs/later.md`: parking lot for tangents.
- `docs/spark-ui.md`: how to keep the Spark UI open (interactive session) or replay finished
  runs (event logs + History Server). Follow it when the user wants to inspect plans/stages.
- `pyspark-databricks-portfolio-plan.md` (repo root, **gitignored, private**, may not exist on
  every machine): goals, session plan, working agreement and decision log. Read it at the start of
  a session when present; never commit it or quote private background from it into the repo.
