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


def traffic_events(bronze: DataFrame) -> DataFrame:
    """Build silver traffic events from a slice of bronze ``traffic_logs``.

    Steps, in order:
    1. Keep ALLOW events only (a business filter, not a validity check).
    2. One ``select``: keep every bronze column and add the parsed ones next to their source
       (invalid values become null; the raw string stays so they can be diagnosed).
    3. Flag rows Spark could not parse: ``is_corrupt``. They are kept, not dropped.
    4. Remove duplicate ``event_id`` within this slice, keeping one copy deterministically.
       Duplicates against events already in silver are the MERGE's job, not this function's.
    """
    allowed = bronze.where(F.col("action") == C.ACTION_ALLOW)

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
        F.col(CORRUPT_COLUMN).isNotNull().alias("is_corrupt"),
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
    return ranked.where((F.col("_copy") == 1) | F.col("event_id").isNull()).drop("_copy")
