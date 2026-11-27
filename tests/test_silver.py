"""Unit tests for the silver transforms, on tiny hand-built DataFrames (plus a few on TINY data)."""

from datetime import UTC, date, datetime

from pyspark.sql import Row
from pyspark.sql import functions as F

from netusage import io
from netusage.transforms.silver import (
    ip_to_long,
    latest_snapshot,
    normalize_email,
    normalize_ports,
    normalize_protocol,
    parse_event_time,
    port_to_int,
    traffic_events,
    utc_date,
)

# Values that make the throwing (non-try) Spark functions fail under ANSI mode.
ANSI_BREAKERS = ["", "   ", "garbage", None]


def _apply(spark, expr, values):
    """Apply a Column -> Column expression to one string column; {input: output}."""
    df = spark.createDataFrame([(v,) for v in values], "raw string")
    return {r.raw: r.out for r in df.select("raw", expr("raw").alias("out")).collect()}


def _epoch(*args):
    """Seconds since the epoch of a UTC wall-clock time (tz-free, unlike collected timestamps)."""
    return int(datetime(*args, tzinfo=UTC).timestamp())


def _normalize(spark, ports):
    """Build a one-column DataFrame from raw port strings and normalize it."""
    return normalize_ports(spark.createDataFrame([(p,) for p in ports], "port string"))


def _as_dict(df):
    """Collect as {raw_port: (start, end, flag)} so row order never matters."""
    return {
        r.port: (r.port_start, r.port_end, r.port_spec_valid)
        for r in df.select("port", "port_start", "port_end", "port_spec_valid").collect()
    }


def test_valid_single_port_and_range(spark):
    result = _as_dict(_normalize(spark, ["99", "1-9"]))

    assert result == {
        "99": (99, 99, True),
        "1-9": (1, 9, True),
    }


def test_invalid_values_do_not_throw_and_are_flagged(spark):
    # "99999999999" overflows try_cast; the flag must be False, not NULL.
    ports = [
        # malformed
        "abc",
        "80-abc",
        "-20",
        "80-",
        "1-2-3",
        "",
        "   ",
        None,
        # out of range (port 0 is reserved)
        "0",
        "0-65535",
        "65536",
        "1-65536",
        "65536-65536",
        # start > end
        "100-1",
        # overflow try_cast to NULL
        "99999999999",
        "1-99999999999",
        "99999999999-1",
    ]
    result = _as_dict(_normalize(spark, ports))

    assert result == {port: (None, None, False) for port in ports}


def test_surrounding_whitespace_is_trimmed(spark):
    result = _as_dict(_normalize(spark, ["  1-9  "]))

    assert result == {"  1-9  ": (1, 9, True)}


def test_boundaries_are_inclusive(spark):
    result = _as_dict(_normalize(spark, ["1", "65535", "1-65535"]))

    assert result == {
        "1": (1, 1, True),
        "65535": (65535, 65535, True),
        "1-65535": (1, 65535, True),
    }


def test_output_columns_are_integers_and_raw_port_is_kept(spark):
    df = spark.createDataFrame([(" 443 ",), (" 1-20 ",)], ["port"])

    result = normalize_ports(df)

    expected = [
        Row(port=" 443 ", port_start=443, port_end=443, port_spec_valid=True),
        Row(port=" 1-20 ", port_start=1, port_end=20, port_spec_valid=True),
    ]

    assert result.collect() == expected


def test_invalid_port_values_are_flagged(spark):
    """Malformed port values should be retained with null bounds and a false flag."""

    df = spark.createDataFrame(
        [
            ("abc",),
            ("80-abc",),
            ("-20",),
            ("80-",),
            ("80-90-100",),
            ("",),
            ("   ",),
        ],
        ["port"],
    )

    result = normalize_ports(df)

    assert _as_dict(result) == {
        port: (None, None, False)
        for port in ["abc", "80-abc", "-20", "80-", "80-90-100", "", "   "]
    }


def test_integer_overflow_values_are_flagged(spark):
    """Values that overflow INT should be flagged, not crash the cast (ANSI mode)."""

    df = spark.createDataFrame(
        [
            ("99999999999",),
            ("1-99999999999",),
            ("99999999999-1",),
        ],
        ["port"],
    )

    result = normalize_ports(df)

    assert _as_dict(result) == {
        port: (None, None, False) for port in ["99999999999", "1-99999999999", "99999999999-1"]
    }


def test_out_of_range_ports_are_flagged(spark):
    """Ports outside 1-65535 should be retained with a false validity flag."""

    df = spark.createDataFrame(
        [
            ("0",),
            ("0-65535",),
            ("-1",),
            ("65536",),
            ("1-65536",),
            ("65536-65536",),
        ],
        ["port"],
    )

    result = normalize_ports(df)

    assert _as_dict(result) == {
        port: (None, None, False)
        for port in ["0", "0-65535", "-1", "65536", "1-65536", "65536-65536"]
    }


def test_invalid_range_order_is_flagged(spark):
    """A range where start is greater than end should be retained and flagged."""

    df = spark.createDataFrame(
        [
            ("20-10",),
            ("100-50",),
        ],
        ["port"],
    )

    result = normalize_ports(df)

    assert _as_dict(result) == {
        "20-10": (None, None, False),
        "100-50": (None, None, False),
    }


def test_mixed_valid_and_invalid_ports(spark):
    """Valid and invalid rows should remain with normalized values or invalid flags."""

    df = spark.createDataFrame(
        [
            ("49",),
            ("1-20",),
            ("invalid",),
            ("65536",),
            ("100-200",),
            ("99999999999",),
        ],
        ["port"],
    )

    result = normalize_ports(df)

    assert _as_dict(result) == {
        "49": (49, 49, True),
        "1-20": (1, 20, True),
        "invalid": (None, None, False),
        "65536": (None, None, False),
        "100-200": (100, 200, True),
        "99999999999": (None, None, False),
    }


def test_original_port_column_is_preserved(spark):
    """The raw port column should remain alongside the normalized columns."""

    df = spark.createDataFrame([("49",)], ["port"])

    result = normalize_ports(df)

    assert "port" in result.columns
    assert "port_start" in result.columns
    assert "port_end" in result.columns
    assert "port_spec_valid" in result.columns
    assert result.select("port", "port_start", "port_end", "port_spec_valid").first() == Row(
        port="49", port_start=49, port_end=49, port_spec_valid=True
    )


def test_output_columns_are_integers(spark):
    """Normalized columns should have IntegerType."""

    df = spark.createDataFrame([("1-20",)], ["port"])

    result = normalize_ports(df)

    assert result.schema["port_start"].dataType.simpleString() == "int"
    assert result.schema["port_end"].dataType.simpleString() == "int"


def test_ip_to_long_known_values(spark):
    """A dotted-decimal IP converts to a*256**3 + b*256**2 + c*256 + d."""

    df = spark.createDataFrame([("10.0.0.1",), ("1.2.3.4",)], ["destination_ip"])

    result = df.select(ip_to_long("destination_ip").alias("ip_long"))

    expected = [
        Row(ip_long=167772161),
        Row(ip_long=16909060),
    ]

    assert result.collect() == expected


def test_ip_to_long_boundaries(spark):
    """0.0.0.0 and 255.255.255.255 are the valid boundaries."""

    df = spark.createDataFrame([("0.0.0.0",), ("255.255.255.255",)], ["destination_ip"])

    result = df.select(ip_to_long("destination_ip").alias("ip_long"))

    expected = [
        Row(ip_long=0),
        Row(ip_long=4294967295),
    ]

    assert result.collect() == expected


def test_ip_to_long_above_int_range(spark):
    """Addresses above 127.255.255.255 need long arithmetic: int math would overflow."""

    df = spark.createDataFrame([("192.168.1.1",)], ["destination_ip"])

    result = df.select(ip_to_long("destination_ip").alias("ip_long"))

    assert result.first() == Row(ip_long=3232235777)


def test_ip_to_long_trims_whitespace(spark):
    """Whitespace-padded IPs are trimmed, same decision as the port transform."""

    df = spark.createDataFrame([(" 10.0.0.1 ",)], ["destination_ip"])

    result = df.select(ip_to_long("destination_ip").alias("ip_long"))

    assert result.first() == Row(ip_long=167772161)


def test_ip_to_long_invalid_returns_null(spark):
    """Malformed or unparseable IPs return null instead of raising; the caller filters."""

    df = spark.createDataFrame(
        [
            ("256.0.0.1",),
            ("10.0.0",),
            ("10.0.0.",),
            ("10..0.1",),
            ("10.0.0.1.5",),
            ("a.b.c.d",),
            ("",),
            ("   ",),
            (None,),
            ("99999999999999999999.1.1.1",),  # digits that overflow a long: try_cast -> null
        ],
        ["destination_ip"],
    )

    result = df.select(ip_to_long("destination_ip").alias("ip_long"))

    assert result.collect() == [Row(ip_long=None) for _ in range(10)]


def test_ip_to_long_rejects_leading_zeros(spark):
    """Leading zeros are rejected as ambiguous (octal); see the function docstring."""

    df = spark.createDataFrame(
        [
            ("010.0.0.1",),
            ("10.01.0.1",),
            ("00.0.0.0",),
        ],
        ["destination_ip"],
    )

    result = df.select(ip_to_long("destination_ip").alias("ip_long"))

    assert result.collect() == [Row(ip_long=None) for _ in range(3)]


def test_ip_to_long_returns_long_type(spark):
    """The converted column is LongType, matching resource_resolution.ip_start/ip_end."""

    df = spark.createDataFrame([("10.0.0.1",)], ["destination_ip"])

    result = df.select(ip_to_long("destination_ip").alias("ip_long"))

    assert result.schema["ip_long"].dataType.simpleString() == "bigint"


def test_traffic_ips_fall_in_resolution_ranges(spark, tiny_landing):
    """Confidence check on real pipeline data: every traffic IP matches a resolution range.

    The generator picks each event's destination IP from the resource's IP range at the
    event's instant, so ip_to_long(traffic_logs.destination_ip) must land inside some
    (ip_start, ip_end) in resource_resolution. The planted malformed rows only append to the
    last column (action), so their destination_ip values are unaffected and the check can run
    on the whole read without the corrupt-record caveat from tests/test_io.py.
    """
    root = tiny_landing.root

    ips = (
        io.read_landing(spark, root, io.EVENT_ENTITY)
        .select(ip_to_long("destination_ip").alias("ip"))
        .distinct()
    )
    # The generator writes canonical dotted-decimal, so the conversion must never yield null.
    assert ips.where(F.col("ip").isNull()).count() == 0

    ranges = io.read_landing(spark, root, "resource_resolution").select("ip_start", "ip_end")
    unmatched = ips.join(
        ranges,
        (ips["ip"] >= ranges["ip_start"]) & (ips["ip"] <= ranges["ip_end"]),
        "left_anti",
    )

    assert ips.count() > 0
    assert unmatched.count() == 0


# --- event_time -> timestamp, event_date ----------------------------------------------------
# Timestamps are compared as unix_seconds: collect() turns TimestampType into a naive datetime
# in the Python process's local zone, which would make the tests machine-dependent.


def _event_seconds(spark, values):
    return _apply(spark, lambda c: F.unix_seconds(parse_event_time(c)), values)


def test_parse_event_time_utc_and_offset(spark):
    result = _event_seconds(
        spark, ["2025-01-01T03:00:00Z", "2025-01-01T05:00:00+02:00", " 2025-01-01T03:00:00Z "]
    )

    # All three are the same instant: offsets are applied, whitespace is trimmed.
    assert result == {
        "2025-01-01T03:00:00Z": _epoch(2025, 1, 1, 3),
        "2025-01-01T05:00:00+02:00": _epoch(2025, 1, 1, 3),
        " 2025-01-01T03:00:00Z ": _epoch(2025, 1, 1, 3),
    }


def test_parse_event_time_invalid_returns_null(spark):
    values = [
        *ANSI_BREAKERS,
        "2025-01-01T03:00:00",  # no offset: would silently depend on the session time zone
        "2025-01-01 03:00:00Z",  # space instead of T
        "2025-01-01T03:00:00.123Z",  # fractional seconds: not the landing format
        "2025-01-01",
        "2025-02-30T00:00:00Z",  # impossible date
        "2025-01-01T25:00:00Z",  # impossible hour
        "2025-13-01T00:00:00Z",
    ]

    assert _event_seconds(spark, values) == {v: None for v in values}


def test_parse_event_time_returns_timestamp_type(spark):
    df = spark.createDataFrame([("2025-01-01T03:00:00Z",)], "event_time string")

    result = df.select(parse_event_time("event_time").alias("ts"))

    assert result.schema["ts"].dataType.simpleString() == "timestamp"


def test_utc_date_of_parsed_event_time(spark):
    values = ["2025-01-01T00:00:00Z", "2025-01-01T23:59:59Z", "2025-01-02T00:30:00+02:00", None]

    result = _apply(spark, lambda c: utc_date(parse_event_time(c)), values)

    assert result == {
        "2025-01-01T00:00:00Z": date(2025, 1, 1),
        "2025-01-01T23:59:59Z": date(2025, 1, 1),
        "2025-01-02T00:30:00+02:00": date(2025, 1, 1),  # 22:30 UTC the day before
        None: None,
    }


def test_utc_date_before_epoch_uses_floor(spark):
    """floor, not truncation toward zero: -1 s is 1969-12-31, not 1970-01-01."""
    result = _apply(spark, lambda c: utc_date(parse_event_time(c)), ["1969-12-31T23:59:59Z"])

    assert result == {"1969-12-31T23:59:59Z": date(1969, 12, 31)}


def test_event_time_and_date_ignore_session_time_zone(spark):
    """In Los Angeles 03:00Z is still Dec 31: F.to_date(ts) would put it in the wrong partition."""
    key = "spark.sql.session.timeZone"
    original = spark.conf.get(key)
    spark.conf.set(key, "America/Los_Angeles")
    try:
        df = spark.createDataFrame([("2025-01-01T03:00:00Z",)], "event_time string")
        ts = parse_event_time("event_time")
        row = df.select(F.unix_seconds(ts).alias("s"), utc_date(ts).alias("d")).first()
    finally:
        spark.conf.set(key, original)

    assert row == Row(s=_epoch(2025, 1, 1, 3), d=date(2025, 1, 1))


# --- destination_port -> int ---------------------------------------------------------------


def test_port_to_int_valid(spark):
    result = _apply(spark, port_to_int, ["1", "443", "65535", " 8080 ", "080"])

    assert result == {"1": 1, "443": 443, "65535": 65535, " 8080 ": 8080, "080": 80}


def test_port_to_int_invalid_returns_null(spark):
    values = [
        *ANSI_BREAKERS,
        "0",  # reserved
        "65536",
        "-1",  # try_cast would accept it
        "+80",  # try_cast would accept it
        "80.0",
        "1e3",
        "80-90",  # a range is a rule port spec, not an event port
        "99999999999",  # overflows int: cast would throw
    ]

    assert _apply(spark, port_to_int, values) == {v: None for v in values}


def test_port_to_int_returns_integer_type(spark):
    df = spark.createDataFrame([("443",)], "destination_port string")

    result = df.select(port_to_int("destination_port").alias("port"))

    assert result.schema["port"].dataType.simpleString() == "int"


# --- user_email, protocol -------------------------------------------------------------------


def test_normalize_email(spark):
    values = ["User@Corp.Example", "  user@corp.example  ", "user@corp.example", "garbage"]

    result = _apply(spark, normalize_email, values)

    assert result == {
        "User@Corp.Example": "user@corp.example",
        "  user@corp.example  ": "user@corp.example",
        "user@corp.example": "user@corp.example",
        "garbage": "garbage",  # normalized, not validated: it simply matches no user
    }


def test_normalize_email_blank_is_null(spark):
    assert _apply(spark, normalize_email, ["", "   ", None]) == {"": None, "   ": None, None: None}


def test_normalize_protocol(spark):
    result = _apply(spark, normalize_protocol, ["tcp", " Udp ", "TCP", "garbage"])

    assert result == {"tcp": "TCP", " Udp ": "UDP", "TCP": "TCP", "garbage": "GARBAGE"}


def test_normalize_protocol_blank_is_null(spark):
    result = _apply(spark, normalize_protocol, ["", "   ", None])

    assert result == {"": None, "   ": None, None: None}


def test_traffic_log_columns_parse_on_generated_data(spark, tiny_landing):
    """Confidence check: no generated event loses its time, port, email or protocol.

    Malformed CSV rows (an extra field) are excluded via _corrupt_record: their values are
    unreliable by design. Every other row must parse, and event_date must equal the landing
    date for on-time events (late events land in a later batch_date folder).
    """
    events = io.read_landing(spark, tiny_landing.root, io.EVENT_ENTITY).cache()
    parsed = events.where(F.col(io.CORRUPT_COLUMN).isNull()).select(
        parse_event_time("event_time").alias("ts"),
        port_to_int("destination_port").alias("port"),
        normalize_email("user_email").alias("email"),
        normalize_protocol("protocol").alias("protocol"),
        io.BATCH_COLUMN,
    )
    failed = parsed.where(
        F.col("ts").isNull()
        | F.col("port").isNull()
        | F.col("email").isNull()
        | ~F.col("protocol").isin("TCP", "UDP")
    )
    on_time = parsed.where(utc_date(F.col("ts")) == F.col(io.BATCH_COLUMN))

    assert parsed.count() > 0
    assert failed.count() == 0
    assert on_time.count() > 0
    assert parsed.where(utc_date(F.col("ts")) > F.col(io.BATCH_COLUMN)).count() == 0
    events.unpersist()


# --- traffic_events ------------------------------------------------------------------------------

BRONZE_TRAFFIC = (
    "event_id string, event_time string, user_email string, destination_ip string, "
    "destination_port string, protocol string, action string, _corrupt_record string, "
    "source_file string, batch_date date, ingest_ts timestamp"
)
INGEST_TS = datetime(2025, 1, 10, tzinfo=UTC)


def _bronze_event(
    event_id="evt1",
    action="ALLOW",
    batch_date=date(2025, 1, 1),
    corrupt=None,
    event_time="2025-01-01T03:00:00Z",
    port="443",
):
    """One bronze traffic row with valid defaults; override only what a test is about."""
    source = f"traffic_logs/batch_date={batch_date}/part-0.csv"
    return (
        event_id,
        event_time,
        " Alice@Corp.Example ",
        "10.0.0.1",
        port,
        "tcp",
        action,
        corrupt,
        source,
        batch_date,
        INGEST_TS,
    )


def _events(spark, *rows):
    return traffic_events(spark.createDataFrame(list(rows), BRONZE_TRAFFIC))


def test_traffic_events_parsed_take_plain_names_raw_kept(spark):
    row = _events(spark, _bronze_event()).first()

    # Parsed values take the plain names...
    assert row.event_date == date(2025, 1, 1)
    assert row.user_email == "alice@corp.example"
    assert row.destination_ip == 167772161
    assert row.destination_port == 443
    assert row.protocol == "TCP"
    # ...and the bronze values are kept unchanged as *_raw.
    assert row.event_time_raw == "2025-01-01T03:00:00Z"
    assert row.user_email_raw == " Alice@Corp.Example "
    assert row.destination_ip_raw == "10.0.0.1"
    assert row.destination_port_raw == "443"
    assert row.protocol_raw == "tcp"
    assert row.is_corrupt is False


def test_traffic_events_schema(spark):
    types = dict(_events(spark, _bronze_event()).dtypes)

    assert types["event_ts"] == "timestamp"
    assert types["event_date"] == "date"
    assert types["user_email"] == "string"
    assert types["destination_ip"] == "bigint"
    assert types["destination_port"] == "int"
    assert types["destination_ip_raw"] == "string"
    assert types["destination_port_raw"] == "string"
    assert types["is_corrupt"] == "boolean"
    assert "_copy" not in types  # the dedup helper column does not leak


def test_traffic_events_keeps_allow_only(spark):
    result = _events(
        spark,
        _bronze_event("a", action="ALLOW"),
        _bronze_event("d", action="DENY"),
        _bronze_event("n", action=None),
    )

    assert [r.event_id for r in result.collect()] == ["a"]


def test_traffic_events_flags_corrupt_rows_without_dropping_them(spark):
    result = _events(
        spark,
        _bronze_event("ok"),
        _bronze_event("bad", corrupt="evt,...,ALLOW,EXTRA"),
    )

    assert {r.event_id: r.is_corrupt for r in result.collect()} == {"ok": False, "bad": True}


def test_traffic_events_bad_values_become_null_not_errors(spark):
    """ANSI breakers in every parsed column: the row survives with nulls, the job does not fail."""
    row = _events(spark, _bronze_event(event_time="garbage", port="")).first()

    assert row.event_ts is None
    assert row.event_date is None
    assert row.destination_port is None
    assert row.destination_port_raw == ""


def test_traffic_events_dedup_keeps_earliest_batch(spark):
    result = _events(
        spark,
        _bronze_event("dup", batch_date=date(2025, 1, 2)),
        _bronze_event("dup", batch_date=date(2025, 1, 1)),
        _bronze_event("other"),
    )

    rows = {r.event_id: r.batch_date for r in result.collect()}
    assert rows == {"dup": date(2025, 1, 1), "other": date(2025, 1, 1)}


def test_traffic_events_dedup_prefers_clean_copy_over_earlier_corrupt_one(spark):
    result = _events(
        spark,
        _bronze_event("dup", batch_date=date(2025, 1, 1), corrupt="raw text"),
        _bronze_event("dup", batch_date=date(2025, 1, 2)),
    )

    row = result.first()
    assert result.count() == 1
    assert (row.batch_date, row.is_corrupt) == (date(2025, 1, 2), False)


def test_traffic_events_does_not_collapse_null_event_ids(spark):
    result = _events(spark, _bronze_event(None), _bronze_event(None), _bronze_event("x"))

    assert result.count() == 3


# --- latest_snapshot -----------------------------------------------------------------------------

SNAPSHOT = "sys_id string, name string, batch_date date"


def test_latest_snapshot_keeps_only_newest_batch(spark):
    df = spark.createDataFrame(
        [
            ("a", "old", date(2025, 1, 1)),
            ("a", "new", date(2025, 1, 3)),
            ("b", "new", date(2025, 1, 3)),
            ("c", "mid", date(2025, 1, 2)),  # gone from the newest snapshot: dropped
        ],
        SNAPSHOT,
    )

    result = sorted(tuple(r) for r in latest_snapshot(df).collect())

    assert result == [("a", "new", date(2025, 1, 3)), ("b", "new", date(2025, 1, 3))]


def test_latest_snapshot_single_snapshot_is_unchanged(spark):
    df = spark.createDataFrame(
        [("a", "x", date(2025, 1, 1)), ("b", "y", date(2025, 1, 1))], SNAPSHOT
    )

    assert sorted(latest_snapshot(df).collect()) == sorted(df.collect())


def test_latest_snapshot_keeps_columns_and_types(spark):
    df = spark.createDataFrame([("a", "x", date(2025, 1, 1))], SNAPSHOT)

    assert latest_snapshot(df).schema == df.schema


def test_latest_snapshot_empty_input_gives_empty_output(spark):
    df = spark.createDataFrame([], SNAPSHOT)

    assert latest_snapshot(df).count() == 0
