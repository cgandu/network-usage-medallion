"""Silver transforms: pure functions that normalize bronze columns.

Bad values are flagged in an output column, not dropped here; callers decide
whether to quarantine them or let downstream filters exclude them.

The table transforms (e.g. ``traffic_events``) do remove rows, but only for business
filters (ALLOW events only, active users and rules) and deduplication, never because a
value failed to parse.
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from netusage import config as C
from netusage.io import BATCH_COLUMN, CORRUPT_COLUMN, SOURCE_COLUMN

PORT_SPEC = r"^[0-9]+(?:-[0-9]+)?\z"


def _as_col(c: Column | str) -> Column:
    """Accept a column name or a Column, so expressions are not tied to a column name."""
    return F.col(c) if isinstance(c, str) else c


def _is_corrupt() -> Column:
    """True when Spark could not parse the landing row (its raw text is in ``_corrupt_record``)."""
    return F.col(CORRUPT_COLUMN).isNotNull()


def normalize_ports(df: DataFrame) -> DataFrame:
    """Normalize a raw ``port`` string into integer start/end columns.

    Valid formats are ``N`` and ``N-M`` with 1 <= N <= M <= 65535. The original
    ``port`` column is preserved. Invalid rows get null start/end values and
    ``port_spec_valid = false``; they are flagged, not dropped. ``coalesce``
    keeps the flag strictly boolean when an all-digit value overflows
    ``try_cast("int")`` to null.
    """
    port = F.trim(F.col("port"))
    parts = F.split(port, "-")
    start = F.get(parts, 0).try_cast("int")
    # Branch on structure, not on a NULL: try_cast also returns NULL when the second part exists
    # but overflows, and coalesce(end, start) would turn "1-99999999999" into the valid port 1.
    end = F.when(F.size(parts) > 1, F.get(parts, 1).try_cast("int")).otherwise(start)

    port_spec_valid = F.coalesce(
        (
            F.col("port").isNotNull()
            & port.rlike(PORT_SPEC)
            & start.between(1, 65535)
            & end.between(1, 65535)
            & (start <= end)
        ),
        F.lit(False),
    )
    return (
        df.withColumn("port_start", F.when(port_spec_valid, start))
        .withColumn("port_end", F.when(port_spec_valid, end))
        .withColumn("port_spec_valid", port_spec_valid)
    )


# An octet with no leading zeros: "0" and "10" match, "00" and "010" do not. Leading zeros are
# ambiguous ("010" is 10 in decimal but 8 in octal in some parsers), so they are rejected
# outright; the generator writes canonical dotted-decimal.
_IP_OCTET = r"(0|[1-9]\d*)"
_IP_REGEX = rf"^{_IP_OCTET}\.{_IP_OCTET}\.{_IP_OCTET}\.{_IP_OCTET}$"


def ip_to_long(ip: Column | str) -> Column:
    """
    Convert a dotted-decimal IPv4 string column into its LongType integer value.

    "a.b.c.d" -> a*256**3 + b*256**2 + c*256 + d, e.g. "10.0.0.1" -> 167772161. The result is
    LongType because addresses above 127.255.255.255 overflow a signed int ("192.168.1.1" ->
    3232235777), which is why resource_resolution.ip_start/ip_end are longs too. Each octet is
    try_cast to long BEFORE multiplying, so int arithmetic can never overflow.

    Decisions (documented so callers and tests can rely on them):
    - Surrounding whitespace is trimmed, same as the port transform (" 10.0.0.1 " is valid).
    - Leading zeros ("010.0.0.1") are rejected: canonical dotted-decimal has none, they are
      ambiguous with octal, and the generator never writes them.
    - Anything invalid (wrong shape, octet > 255, digits overflowing a long) returns null
      instead of raising: try_cast and F.get return null on failure (cast overflow, missing
      array index), and the null comparisons reject the row even under ANSI mode. Returning
      null (rather than filtering) keeps this composable with the other silver expressions in
      one select, and leaves the quarantine decision to the caller.

    Unlike normalize_port, this is not tied to a column name.
    """
    trimmed = F.trim(_as_col(ip))
    # F.get (not getItem): under ANSI mode, getItem throws INVALID_ARRAY_INDEX on a missing
    # index — inputs like "" or "10.0.0" split into fewer than 4 parts — and Spark may hoist it
    # out of the AND chain via common subexpression elimination, so a guard beside it is not
    # enough (the port function only survives getItem because it sits inside F.when's branch).
    # F.get returns null instead. Rule of thumb under ANSI: anything that can throw (cast,
    # getItem, element_at, division, ...) needs its try_/null-safe version.
    parts = F.split(trimmed, r"\.")
    octets = [F.get(parts, i).try_cast("long") for i in range(4)]

    # The regex only allows digits, so octets can never be negative; only the 255 cap matters.
    is_valid_ip = trimmed.rlike(_IP_REGEX)
    for octet in octets:
        is_valid_ip &= octet <= 255
    value = (
        octets[0] * F.lit(256**3) + octets[1] * F.lit(256**2) + octets[2] * F.lit(256) + octets[3]
    )
    # No otherwise: invalid or null input yields null.
    return F.when(is_valid_ip, value)


# ISO 8601 with a mandatory offset: "2025-01-01T03:00:00Z" or "2025-01-01T03:00:00+02:00".
# XXX parses "Z" and "+HH:MM". A string with no offset fails to parse (null) instead of being
# read in spark.sql.session.timeZone, so the instant never depends on session config.
EVENT_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"


def parse_event_time(event_time: Column | str) -> Column:
    """Parse an ISO 8601 event-time string into a TimestampType instant (null if invalid).

    ``try_to_timestamp`` returns null where ``to_timestamp`` throws CANNOT_PARSE_TIMESTAMP under
    ANSI mode: garbage, ``""``, impossible dates ("2025-02-30"), a missing offset, or a different
    shape (space instead of ``T``, fractional seconds). Surrounding whitespace is trimmed, same
    as the other silver expressions.

    TimestampType stores an instant (UTC microseconds since the epoch); the session time zone
    only affects how it is displayed or converted to a date, never the stored value.
    """
    return F.try_to_timestamp(F.trim(_as_col(event_time)), F.lit(EVENT_TIME_FORMAT))


_SECONDS_PER_DAY = 86_400


def utc_date(ts: Column | str) -> Column:
    """The UTC calendar date of a TimestampType instant (null for null input).

    Not ``F.to_date(ts)``: that converts in ``spark.sql.session.timeZone``, so the same event
    would land in a different ``event_date`` partition on a session set to another zone.
    ``unix_seconds`` counts seconds since 1970-01-01T00:00:00Z regardless of the session zone;
    ``floor`` (not truncation) keeps pre-1970 instants on the right day.
    """
    days = F.floor(F.unix_seconds(_as_col(ts)) / _SECONDS_PER_DAY)
    return F.date_from_unix_date(days.cast("int"))


_PORT_DIGITS = r"^[0-9]+\z"


def port_to_int(port: Column | str) -> Column:
    """Convert a single destination-port string to IntegerType, valid range 1–65535 (else null).

    ``try_cast`` returns null where ``cast`` throws under ANSI mode (``""``, ``"abc"``,
    ``"80.0"``, digits overflowing an int). The digits-only regex is checked as well because
    ``try_cast`` is lenient in ways we do not want: it accepts ``"+80"`` and ``"-1"``. Both
    checks sit in the ``when`` condition and the value is only produced inside the branch;
    nothing here can throw, so evaluation order does not matter. Leading zeros ("080") are
    accepted, same as ``PORT_SPEC`` for rule ports. Port 0 is reserved, so it is invalid.
    """
    trimmed = F.trim(_as_col(port))
    value = trimmed.try_cast("int")
    is_valid = trimmed.rlike(_PORT_DIGITS) & value.between(1, 65535)
    # No otherwise: invalid or null input yields null.
    return F.when(is_valid, value)


def normalize_email(email: Column | str) -> Column:
    """``lower(trim(email))``; blank becomes null. Use on both sides of the users join.

    Emails are compared case-insensitively in practice and the generator plants upper-case and
    space-padded variants. An empty string is not an email: mapping it to null makes "missing"
    one value (null) instead of two (null and ``""``), so null checks catch both.
    """
    return F.nullif(F.lower(F.trim(_as_col(email))), F.lit(""))


def normalize_protocol(protocol: Column | str) -> Column:
    """``upper(trim(protocol))``; blank becomes null. Matches rule protocols ("TCP", "UDP").

    Normalizes only; it does not check the value is a known protocol. An unknown protocol
    simply matches no rule in the join, which is a business outcome, not a parse failure.
    """
    return F.nullif(F.upper(F.trim(_as_col(protocol))), F.lit(""))


def latest_snapshot(bronze: DataFrame) -> DataFrame:
    """Keep only the rows of the newest ``batch_date`` in a bronze snapshot table.

    Bronze keeps one full snapshot of users, apps, rules, groups, memberships and resolutions
    per run date; silver uses the latest one. The input's columns come back unchanged.

    How: compute the max ``batch_date`` as a 1-row DataFrame and semi-join on it. Everything
    stays in one lazy plan (no ``collect()`` to the driver), and broadcasting the 1-row side
    avoids moving all rows to one partition, as a ``Window`` without ``partitionBy`` would.
    ``left_semi`` keeps the left rows that have a match and adds no columns from the right.
    Rows with a null ``batch_date`` never match (``max`` ignores nulls and ``=`` with null is
    not true); bronze always sets it.

    The final ``select`` restores the input's column order: a join ``on`` a column name puts
    that column first, even for ``left_semi``.
    """
    newest = bronze.agg(F.max(BATCH_COLUMN).alias(BATCH_COLUMN))
    latest = bronze.join(F.broadcast(newest), on=BATCH_COLUMN, how="left_semi")
    return latest.select(*bronze.columns)


def traffic_events(bronze: DataFrame, all_users: DataFrame) -> DataFrame:
    """Build silver traffic events from a slice of bronze ``traffic_logs``.

    Inputs:
    - ``bronze``: a slice of bronze ``traffic_logs`` (the rows transformed).
    - ``all_users``: EVERY user in the directory, active or not, e.g. ``latest_snapshot`` of
      bronze ``users`` (only ``email`` is used). Not silver ``users``: a disabled user's event
      comes from a real person, so it is not an unknown user.

    Steps, in order:
    1. Keep ALLOW events only (a business filter, not a validity check). Corrupt rows are kept
       too: a CSV row with too few fields has null trailing columns, ``action`` included, so
       the filter alone would drop it silently.
    2. One ``select``: keep every bronze column and add the parsed ones next to their source
       (invalid values become null; the raw string stays so they can be diagnosed).
    3. Flag rows Spark could not parse: ``is_corrupt``. They are kept, not dropped.
    4. Remove duplicate ``event_id`` within this slice, keeping one copy deterministically.
       Duplicates against events already in silver are the MERGE's job, not this function's.
    5. Flag ``is_unknown_user``: the normalized ``user_email`` matches no user in ``all_users``
       (normalized with the same ``normalize_email``). Flagged, not dropped. A null email
       matches nothing, so it is flagged too.
    """
    allowed = bronze.where((F.col("action") == C.ACTION_ALLOW) | _is_corrupt())

    # The parsed value takes the plain name, so the obvious column is the safe one to join on;
    # the bronze value is kept as <name>_raw for diagnosing nulls.
    event_ts = parse_event_time("event_time")
    parsed = allowed.select(
        "event_id",
        F.col("event_time").alias("event_time_raw"),
        event_ts.alias("event_ts"),
        utc_date(event_ts).alias("event_date"),
        F.col("user_email").alias("user_email_raw"),
        normalize_email("user_email").alias("user_email"),
        F.col("destination_ip").alias("destination_ip_raw"),
        ip_to_long("destination_ip").alias("destination_ip"),
        F.col("destination_port").alias("destination_port_raw"),
        port_to_int("destination_port").alias("destination_port"),
        F.col("protocol").alias("protocol_raw"),
        normalize_protocol("protocol").alias("protocol"),
        "action",
        CORRUPT_COLUMN,
        _is_corrupt().alias("is_corrupt"),
        SOURCE_COLUMN,
        BATCH_COLUMN,
        "ingest_ts",
    )

    # Which copy survives must not depend on how Spark happens to order the rows, or re-running
    # the same slice could store a different copy (a different source_file). So rank explicitly:
    # a clean copy beats a corrupt one, then the earliest batch, then the file name as tie-break.
    # source_file sorts byte by byte ("part-10" < "part-9"): stable, not meaningful, and that is
    # all a tie-break needs. Known gap: two copies in the SAME file tie on every key, so Spark may
    # keep either one. Harmless here (the generator writes one file per batch and its copies are
    # identical); real data would need a per-row key. Parked in docs/later.md.
    first_copy = Window.partitionBy("event_id").orderBy(
        F.col("is_corrupt").asc(), F.col(BATCH_COLUMN).asc(), F.col(SOURCE_COLUMN).asc()
    )
    ranked = parsed.withColumn("_copy", F.row_number().over(first_copy))
    # partitionBy puts every null event_id in ONE group; without the isNull branch, all rows
    # missing an id would collapse into a single row. Keep them all: they are not duplicates.
    deduped = ranked.where((F.col("_copy") == 1) | F.col("event_id").isNull()).drop("_copy")

    # Same pattern as is_orphan in group_members: a left join keeps every event and adds a marker
    # that is null when no user has this email. distinct() prevents fan-out (one email twice in
    # the directory would otherwise duplicate the event). Join after dedup: fewer rows to join.
    known_emails = (
        all_users.select(normalize_email("email").alias("user_email"))
        .distinct()
        .withColumn("_user_found", F.lit(True))
    )
    flagged = deduped.join(known_emails, "user_email", "left")

    # Joining on a column NAME moves user_email to the front (seen in latest_snapshot), so select
    # the order explicitly: the data columns, the new flag, then the metadata columns last.
    metadata = [SOURCE_COLUMN, BATCH_COLUMN, "ingest_ts"]
    return flagged.select(
        *[c for c in deduped.columns if c not in metadata],
        F.col("_user_found").isNull().alias("is_unknown_user"),
        *metadata,
    )


def users(bronze: DataFrame) -> DataFrame:
    """Build silver users from bronze ``users`` (all snapshots).

    Steps, in order:
    1. Keep the latest snapshot only.
    2. Keep active users (``status == USER_STATUS_ACTIVE``), a business filter. Corrupt rows are
       kept too: an unparseable JSON line has every column null, including ``status``, so the
       filter alone would drop it silently. They stay, flagged, for the caller to quarantine.
    3. One ``select``: email normalized under the plain name (the key the traffic join uses,
       built with the same ``normalize_email`` as ``traffic_events.user_email``), the bronze value
       kept as ``email_raw``, and ``is_corrupt`` added.

    Email uniqueness is checked by the generator's validation, not enforced here.
    """
    latest = latest_snapshot(bronze)
    kept = latest.where((F.col("status") == C.USER_STATUS_ACTIVE) | _is_corrupt())
    return kept.select(
        "sys_id",
        F.col("email").alias("email_raw"),
        normalize_email("email").alias("email"),
        "department",
        "site",
        "status",
        CORRUPT_COLUMN,
        _is_corrupt().alias("is_corrupt"),
        SOURCE_COLUMN,
        BATCH_COLUMN,
        "ingest_ts",
    )


def apps(bronze: DataFrame) -> DataFrame:
    """Build silver apps from bronze ``apps`` (all snapshots).

    Latest snapshot plus an ``is_corrupt`` flag. No business filter and nothing to normalize:
    rules reference apps by ``sys_id``, copied as is.
    """
    return latest_snapshot(bronze).select(
        "sys_id",
        "name",
        "department",
        CORRUPT_COLUMN,
        _is_corrupt().alias("is_corrupt"),
        SOURCE_COLUMN,
        BATCH_COLUMN,
        "ingest_ts",
    )


def ad_groups(bronze: DataFrame) -> DataFrame:
    """Build silver AD groups from bronze ``ad_group`` (all snapshots).

    Latest snapshot plus an ``is_corrupt`` flag. ``name`` and ``domain`` are NOT normalized:
    rules reference a group by the exact ``(name, domain)`` pair, and a name alone is not
    unique (the same name exists in several domains).
    """
    return latest_snapshot(bronze).select(
        "group_id",
        "name",
        "domain",
        CORRUPT_COLUMN,
        _is_corrupt().alias("is_corrupt"),
        SOURCE_COLUMN,
        BATCH_COLUMN,
        "ingest_ts",
    )


def vpn_rules(bronze: DataFrame) -> DataFrame:
    """Build silver VPN rules from bronze ``vpn_rules`` (all snapshots).

    Steps, in order:
    1. Keep the latest snapshot only.
    2. Keep operational rules (``operational_status == OPERATIONAL_STATUS_ACTIVE``), a business
       filter; corrupt rows are kept, flagged, for the same reason as in ``users``.
    3. ``normalize_ports`` adds ``port_start``, ``port_end`` and ``port_spec_valid``. Invalid
       specs are flagged, not dropped.
    4. One ``select``: bronze ``port`` and ``protocol`` renamed to ``port_raw`` and
       ``protocol_raw`` (same ``<name>_raw`` convention as ``traffic_events``), protocol
       normalized under the plain name, ``is_corrupt`` added.
    """
    latest = latest_snapshot(bronze)
    kept = latest.where(
        (F.col("operational_status") == C.OPERATIONAL_STATUS_ACTIVE) | _is_corrupt()
    )
    return normalize_ports(kept).select(
        "sys_id",
        "app_sys_id",
        "gateway",
        "resource",
        F.col("port").alias("port_raw"),
        "port_start",
        "port_end",
        "port_spec_valid",
        F.col("protocol").alias("protocol_raw"),
        normalize_protocol("protocol").alias("protocol"),
        "ad_group_name",
        "ad_group_domain",
        "operational_status",
        CORRUPT_COLUMN,
        _is_corrupt().alias("is_corrupt"),
        SOURCE_COLUMN,
        BATCH_COLUMN,
        "ingest_ts",
    )


def group_members(memberships: DataFrame, groups: DataFrame, all_users: DataFrame) -> DataFrame:
    """Build silver group memberships from bronze ``ad_group_membership`` (all snapshots).

    Inputs:
    - ``memberships``: bronze ``ad_group_membership``, all snapshots (the rows transformed).
    - ``groups``: silver ``ad_groups``, a lookup (only ``group_id`` is used).
    - ``all_users``: EVERY user in the directory, active or not, e.g. ``latest_snapshot`` of
      bronze ``users`` (only ``sys_id`` is used). Not silver ``users``: that table holds active
      users only, and a disabled user's membership is not an orphan.

    Steps, in order:
    1. Keep the latest snapshot only.
    2. Keep user members (``member_type == MEMBER_TYPE_USER``), a business filter; corrupt rows
       are kept, flagged, as in the other tables.
    3. Remove duplicate ``(group_id, member_id)`` rows. ``_corrupt_record`` is part of the key,
       so clean rows are deduped among themselves (it is null for all of them) and a corrupt
       row never replaces or hides a clean copy.
    4. Flag ``is_orphan``: the group is not in ``groups`` or the member is not in ``all_users``.
       Flagged, not dropped: the matching joins skip orphans on their own, and gold can count
       them.
    """
    latest = latest_snapshot(memberships)
    kept = latest.where((F.col("member_type") == C.MEMBER_TYPE_USER) | _is_corrupt())
    # dropDuplicates keeps one row per key, but which one is not defined. Fine here: within one
    # snapshot, copies of a membership can differ only in source_file (a diagnostic column).
    deduped = kept.dropDuplicates(["group_id", "member_id", CORRUPT_COLUMN])

    # A left join keeps every membership and adds the marker column; no match -> marker is null.
    # distinct() makes each id appear once on the right: a duplicated key there would multiply
    # the membership rows (fan-out) instead of just flagging them.
    known_groups = groups.select("group_id").distinct().withColumn("_group_found", F.lit(True))
    known_users = (
        all_users.select(F.col("sys_id").alias("member_id"))
        .distinct()
        .withColumn("_member_found", F.lit(True))
    )
    flagged = deduped.join(known_groups, "group_id", "left").join(known_users, "member_id", "left")

    is_orphan = F.col("_group_found").isNull() | F.col("_member_found").isNull()
    return flagged.select(
        "group_id",
        "member_id",
        "group_domain",
        "member_type",
        CORRUPT_COLUMN,
        _is_corrupt().alias("is_corrupt"),
        is_orphan.alias("is_orphan"),
        SOURCE_COLUMN,
        BATCH_COLUMN,
        "ingest_ts",
    )


# The largest IPv4 address as an integer (255.255.255.255), the upper bound of ip_to_long.
IPV4_MAX = 256**4 - 1


def resolution_states(bronze: DataFrame) -> DataFrame:
    """Build silver resolution states from bronze ``resource_resolution`` (all snapshots).

    One row per (resource, state, IP range): a state is the set of IP ranges a resource resolved
    to during ``[first_seen, last_seen)``; ``last_seen`` NULL means the state is current.

    Steps, in order:
    1. Keep the latest snapshot only. Each snapshot holds the full history of states.
    2. Add validity flags; rows are flagged, not dropped:
       - ``ip_range_valid``: both ends present and ``0 <= ip_start <= ip_end <= IPV4_MAX``.
       - ``period_valid``: ``first_seen`` present and, unless the state is current,
         ``first_seen < last_seen`` (half-open: an empty or reversed period can match nothing).

    Parquet is typed, so there is nothing to parse and no ``_corrupt_record``: a file that does
    not match the schema fails the read instead. Disjointness BETWEEN states of a resource is a
    property of the whole table, not of a row; ``validation.py`` and the tests check it.
    """
    ip_start, ip_end = F.col("ip_start"), F.col("ip_end")
    first_seen, last_seen = F.col("first_seen"), F.col("last_seen")

    ip_range_valid = F.coalesce(
        (ip_start >= 0) & (ip_start <= ip_end) & (ip_end <= IPV4_MAX), F.lit(False)
    )
    period_valid = F.coalesce(
        first_seen.isNotNull() & (last_seen.isNull() | (first_seen < last_seen)), F.lit(False)
    )
    return latest_snapshot(bronze).select(
        "resource_name",
        "state_id",
        "ip_start",
        "ip_end",
        "first_seen",
        "last_seen",
        ip_range_valid.alias("ip_range_valid"),
        period_valid.alias("period_valid"),
        SOURCE_COLUMN,
        BATCH_COLUMN,
        "ingest_ts",
    )


def rule_access(rules: DataFrame, states: DataFrame) -> DataFrame:
    """What each rule grants, over time: silver ``vpn_rules`` joined with ``resolution_states``.

    Inputs:
    - ``rules``: silver ``vpn_rules`` (latest snapshot, operational, ports parsed).
    - ``states``: silver ``resolution_states``.

    One row per (rule, state, IP range): a rule whose resource resolved to 3 states with 2
    ranges each gets 6 rows. That multiplication is the point: the matching join (Session 4)
    looks for the row whose IP range, port range, protocol and period contain an event.

    Nothing is dropped; the flags from both inputs are carried through so matching can choose
    the usable rows in one place:
    - ``has_resolution``: false for a rule whose resource has no resolution at all (left join).
      Such a rule keeps one row with null IP and period columns; it can never be used.
    - ``port_spec_valid``, ``ip_range_valid``, ``period_valid``, ``is_corrupt``: as in the
      inputs. The state flags are false (never null) on rows without a resolution.
    """
    # Only the columns rule_access needs, renamed where they would be ambiguous after the join
    # (both tables have source_file, batch_date, ingest_ts; "sys_id" is clearer as rule_sys_id).
    rule_cols = rules.select(
        F.col("sys_id").alias("rule_sys_id"),
        "app_sys_id",
        "gateway",
        "resource",
        "protocol",
        "port_start",
        "port_end",
        "port_spec_valid",
        "ad_group_name",
        "ad_group_domain",
        "is_corrupt",
    )
    state_cols = states.select(
        "resource_name",
        "state_id",
        "ip_start",
        "ip_end",
        "first_seen",
        "last_seen",
        "ip_range_valid",
        "period_valid",
    )
    # The key columns have different names, so join on an expression (both columns are kept;
    # resource_name is dropped below). A null resource matches nothing and keeps its row.
    joined = rule_cols.join(
        state_cols, rule_cols["resource"] == state_cols["resource_name"], "left"
    )

    # resource_name comes from the states side: it is null exactly when no state matched.
    has_resolution = F.col("resource_name").isNotNull()
    return joined.select(
        "rule_sys_id",
        "app_sys_id",
        "gateway",
        "resource",
        "protocol",
        "port_start",
        "port_end",
        "ad_group_name",
        "ad_group_domain",
        "state_id",
        "ip_start",
        "ip_end",
        "first_seen",
        "last_seen",
        has_resolution.alias("has_resolution"),
        "port_spec_valid",
        # Unmatched rows get null here from the left join; keep the flags strictly boolean.
        F.coalesce("ip_range_valid", F.lit(False)).alias("ip_range_valid"),
        F.coalesce("period_valid", F.lit(False)).alias("period_valid"),
        "is_corrupt",
    )
