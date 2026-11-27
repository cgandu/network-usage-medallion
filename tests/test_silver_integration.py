"""Silver end to end on TINY data: bronze Delta tables in, every silver transform chained.

The per-table tests in test_silver.py read landing files and fake the bronze metadata. These
tests run the real bronze job into Delta (twice, so bronze holds two snapshots) and build all
silver tables in the order the silver job will, so they catch what one table cannot:
- the wiring: which input goes where (e.g. ALL users, not silver users, for the two flags),
- latest_snapshot on real Delta data with more than one snapshot,
- the join keys between silver tables: same names' meaning and same types (the matching join
  in Session 4 relies on them),
- foreign keys along the join path, and determinism of a rebuild.
"""

from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import functions as F

from netusage import config as C
from netusage import io
from netusage.jobs import bronze as job
from netusage.transforms.silver import (
    ad_groups,
    apps,
    group_members,
    latest_snapshot,
    resolution_states,
    rule_access,
    traffic_events,
    users,
    vpn_rules,
)

TS = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def build_silver(spark, bronze):
    """Every silver table from bronze Delta, in the order the silver job will build them."""

    def read(entity):
        return io.read_delta(spark, io.table_target(bronze, entity))

    # Every user, any status: decides is_orphan and is_unknown_user. Silver users() keeps
    # active users only, so it must NOT be used for those two flags.
    all_users = latest_snapshot(read("users"))
    groups = ad_groups(read("ad_group"))
    rules = vpn_rules(read("vpn_rules"))
    states = resolution_states(read("resource_resolution"))
    return {
        "users": users(read("users")),
        "apps": apps(read("apps")),
        "ad_groups": groups,
        "vpn_rules": rules,
        "group_members": group_members(read("ad_group_membership"), groups, all_users),
        "resolution_states": states,
        "rule_access": rule_access(rules, states),
        "traffic_events": traffic_events(read("traffic_logs"), all_users),
    }


@pytest.fixture(scope="module")
def bronze(spark, tiny_landing, tmp_path_factory):
    """Bronze Delta tables like a backfill followed by one daily run.

    The backfill loads every day but the last and stamps the snapshots with its end date; the
    daily run loads the last day and stamps a second, newer snapshot. Both snapshots hold the
    same landing data, so a broken latest_snapshot would show up as doubled row counts.
    """
    root, start, end = tiny_landing
    path = str(tmp_path_factory.mktemp("silver_integration") / "bronze")
    job.run(spark, root, path, start, end - timedelta(days=1), ingest_ts=TS)
    job.run(spark, root, path, end, end, ingest_ts=TS + timedelta(days=1))
    return path


@pytest.fixture(scope="module")
def silver(spark, bronze):
    tables = {name: df.cache() for name, df in build_silver(spark, bronze).items()}
    yield tables
    for df in tables.values():
        df.unpersist()


def _values(df, column):
    """The distinct values of one column, as a Python set."""
    return {r[0] for r in df.select(column).distinct().collect()}


SNAPSHOT_TABLES = ("users", "apps", "ad_groups", "vpn_rules", "group_members", "resolution_states")


def test_bronze_holds_two_snapshots_and_silver_uses_the_newest(spark, bronze, silver, tiny_landing):
    end = tiny_landing.end
    bronze_users = io.read_delta(spark, io.table_target(bronze, "users"))
    assert _values(bronze_users, "batch_date") == {end - timedelta(days=1), end}

    for name in SNAPSHOT_TABLES:
        assert _values(silver[name], "batch_date") == {end}, name
    # One snapshot's worth of rows, not two.
    assert silver["apps"].count() == C.TINY.apps
    assert silver["ad_groups"].count() == C.TINY.ad_groups


def test_traffic_events_cover_every_bronze_day(silver, tiny_landing):
    start, end = tiny_landing.start, tiny_landing.end
    days = _values(silver["traffic_events"], "batch_date")

    assert days == {start + timedelta(d) for d in range((end - start).days + 1)}


# Join keys between silver tables, in the order of the matching join path (architecture.md §3).
# Silver renames and parses columns, so the landing contract in test_schemas.py is not enough:
# these are the columns the matching join will actually compare.
SILVER_JOIN_KEYS = (
    ("events.user_email -> users.email", "traffic_events", "user_email", "users", "email"),
    (
        "users.sys_id -> group_members.member_id",
        "users",
        "sys_id",
        "group_members",
        "member_id",
    ),
    (
        "group_members.group_id -> ad_groups.group_id",
        "group_members",
        "group_id",
        "ad_groups",
        "group_id",
    ),
    (
        "rule_access.ad_group_name -> ad_groups.name",
        "rule_access",
        "ad_group_name",
        "ad_groups",
        "name",
    ),
    (
        "rule_access.ad_group_domain -> ad_groups.domain",
        "rule_access",
        "ad_group_domain",
        "ad_groups",
        "domain",
    ),
    ("rule_access.app_sys_id -> apps.sys_id", "rule_access", "app_sys_id", "apps", "sys_id"),
    # The matching predicates: event value within the rule_access range / period.
    (
        "events.destination_ip in [ip_start, ip_end]",
        "traffic_events",
        "destination_ip",
        "rule_access",
        "ip_start",
    ),
    (
        "events.destination_port in [port_start, port_end]",
        "traffic_events",
        "destination_port",
        "rule_access",
        "port_start",
    ),
    (
        "events.protocol = rule_access.protocol",
        "traffic_events",
        "protocol",
        "rule_access",
        "protocol",
    ),
    (
        "events.event_ts in [first_seen, last_seen)",
        "traffic_events",
        "event_ts",
        "rule_access",
        "first_seen",
    ),
)


@pytest.mark.parametrize(
    "left, left_col, right, right_col",
    [k[1:] for k in SILVER_JOIN_KEYS],
    ids=[k[0] for k in SILVER_JOIN_KEYS],
)
def test_silver_join_keys_have_matching_types(silver, left, left_col, right, right_col):
    left_type = dict(silver[left].dtypes)[left_col]
    right_type = dict(silver[right].dtypes)[right_col]

    assert left_type == right_type


def _missing(child, child_cols, parent, parent_cols):
    """Rows of ``child`` whose key has no row in ``parent`` (a foreign-key check)."""
    parent_keys = parent.select(
        *[F.col(p).alias(c) for c, p in zip(child_cols, parent_cols, strict=True)]
    )
    return child.join(parent_keys, list(child_cols), "left_anti")


def test_foreign_keys_hold_on_clean_rows(silver):
    """Steps 3, 4 and 6 of the join path: rows not flagged as bad point at existing rows."""
    members = silver["group_members"].where(~F.col("is_corrupt") & ~F.col("is_orphan"))
    rules = silver["rule_access"].where(~F.col("is_corrupt"))

    assert _missing(members, ["group_id"], silver["ad_groups"], ["group_id"]).count() == 0
    rule_groups = ["ad_group_name", "ad_group_domain"]
    assert _missing(rules, rule_groups, silver["ad_groups"], ["name", "domain"]).count() == 0
    assert _missing(rules, ["app_sys_id"], silver["apps"], ["sys_id"]).count() == 0


def test_flags_wired_with_all_users(silver):
    """Exactly the planted bad rows are flagged; with silver users there would be many more."""
    events = silver["traffic_events"].where(~F.col("is_corrupt"))
    ghosts = events.where(F.col("user_email").startswith("ghost"))
    assert ghosts.count() > 0
    assert events.where("is_unknown_user").count() == ghosts.count()

    members = silver["group_members"]
    planted = members.where(
        F.col("group_id").contains("-missing-") | F.col("member_id").contains("-missing-")
    )
    assert planted.count() > 0
    assert members.where("is_orphan").count() == planted.count()


def test_event_protocols_are_rule_protocols(silver):
    """Both sides normalized by normalize_protocol: every clean event protocol exists in rules."""
    event_protocols = _values(silver["traffic_events"].where(~F.col("is_corrupt")), "protocol")
    rule_protocols = _values(silver["rule_access"], "protocol")

    assert event_protocols <= rule_protocols


def test_rebuilding_silver_gives_identical_tables(spark, bronze, silver):
    """Same bronze in, same silver out: dedup and every flag are deterministic."""
    rebuilt = build_silver(spark, bronze)

    for name, table in silver.items():
        again = rebuilt[name]
        assert table.exceptAll(again).count() == 0, name
        assert again.exceptAll(table).count() == 0, name
