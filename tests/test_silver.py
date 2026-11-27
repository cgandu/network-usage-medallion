"""Unit tests for the silver transforms, on tiny hand-built DataFrames (plus a few on TINY data)."""

from datetime import UTC, date, datetime

from pyspark.sql import Row, Window
from pyspark.sql import functions as F

from netusage import config as C
from netusage import io
from netusage.transforms.bronze import add_ingest_metadata
from netusage.transforms.silver import (
    IPV4_MAX,
    ad_groups,
    apps,
    group_members,
    ip_to_long,
    latest_snapshot,
    normalize_email,
    normalize_ports,
    normalize_protocol,
    parse_event_time,
    port_to_int,
    resolution_states,
    rule_access,
    traffic_events,
    users,
    utc_date,
    vpn_rules,
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
    email=" Alice@Corp.Example ",
):
    """One bronze traffic row with valid defaults; override only what a test is about."""
    source = f"traffic_logs/batch_date={batch_date}/part-0.csv"
    return (
        event_id,
        event_time,
        email,
        "10.0.0.1",
        port,
        "tcp",
        action,
        corrupt,
        source,
        batch_date,
        INGEST_TS,
    )


def _events(spark, *rows, emails=("alice@corp.example",)):
    """Run traffic_events; ``emails`` are the user directory's (raw) emails, all users known."""
    all_users = spark.createDataFrame([(e,) for e in emails], "email string")
    return traffic_events(spark.createDataFrame(list(rows), BRONZE_TRAFFIC), all_users)


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


def test_traffic_events_keeps_corrupt_rows_with_null_action(spark):
    """A CSV row with too few fields has null trailing columns; the ALLOW filter must keep it."""
    short_row = _bronze_event("short", action=None, corrupt="short,2025-01-01T03:00:00Z")
    result = _events(spark, _bronze_event("ok"), short_row, _bronze_event("deny", action="DENY"))

    assert {r.event_id: r.is_corrupt for r in result.collect()} == {"ok": False, "short": True}


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


def test_traffic_events_known_user_matches_after_normalizing_both_sides(spark):
    """Noise on both sides (case, spaces) must not make a real user look unknown."""
    result = _events(spark, _bronze_event(), emails=(" ALICE@corp.example",))

    assert result.first().is_unknown_user is False


def test_traffic_events_flags_unknown_users_without_dropping_them(spark):
    result = _events(
        spark,
        _bronze_event("known"),
        _bronze_event("ghost", email="ghost00000001@corp.example"),
        _bronze_event("no_email", email=None),
        _bronze_event("blank", email="   "),  # normalize_email makes it null
    )

    flags = {r.event_id: r.is_unknown_user for r in result.collect()}
    assert flags == {"known": False, "ghost": True, "no_email": True, "blank": True}


def test_traffic_events_duplicate_directory_emails_do_not_multiply_events(spark):
    """Two directory rows that normalize to the same email: still one row per event."""
    emails = ("alice@corp.example", " Alice@Corp.Example")
    result = _events(spark, _bronze_event("a"), _bronze_event("b"), emails=emails)

    assert result.count() == 2


def test_traffic_events_column_order(spark):
    """The join must not move user_email first; the flag sits just before the metadata."""
    columns = _events(spark, _bronze_event()).columns

    assert columns[:2] == ["event_id", "event_time_raw"]
    assert columns[-5:] == [
        "is_corrupt",
        "is_unknown_user",
        "source_file",
        "batch_date",
        "ingest_ts",
    ]
    assert "_user_found" not in columns


def test_traffic_events_unknown_users_on_generated_data(spark, tiny_landing):
    """Exactly the planted ghost emails are unknown; disabled users' events are not."""
    bronze = io.read_traffic_batches(spark, tiny_landing.root, tiny_landing.start, tiny_landing.end)
    bronze = add_ingest_metadata(bronze, INGEST_TS)
    all_users = latest_snapshot(_tiny_bronze(spark, tiny_landing, "users"))  # any status
    events = traffic_events(bronze, all_users).where(~F.col("is_corrupt")).cache()

    ghosts = events.where(F.col("user_email").startswith("ghost"))
    assert ghosts.count() > 0
    assert ghosts.where(~F.col("is_unknown_user")).count() == 0
    assert events.where("is_unknown_user").count() == ghosts.count()  # and nothing else is
    events.unpersist()


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


# --- users ---------------------------------------------------------------------------------------

BRONZE_USERS = (
    "sys_id string, email string, department string, site string, status int, "
    "_corrupt_record string, source_file string, batch_date date, ingest_ts timestamp"
)
SNAPSHOT_DATE = date(2025, 1, 5)


def _bronze_user(
    sys_id="u1",
    email=" Alice@Corp.Example ",
    status=C.USER_STATUS_ACTIVE,
    corrupt=None,
    batch_date=SNAPSHOT_DATE,
):
    """One bronze users row with valid defaults; override only what a test is about."""
    return (
        sys_id,
        email,
        "IT",
        "site-01",
        status,
        corrupt,
        "users/part-0.json",
        batch_date,
        INGEST_TS,
    )


def _users(spark, *rows):
    return users(spark.createDataFrame(list(rows), BRONZE_USERS))


def test_users_email_normalized_raw_kept(spark):
    row = _users(spark, _bronze_user()).first()

    assert row.email == "alice@corp.example"
    assert row.email_raw == " Alice@Corp.Example "
    assert row.is_corrupt is False


def test_users_keeps_active_only(spark):
    result = _users(
        spark,
        _bronze_user("active", status=C.USER_STATUS_ACTIVE),
        _bronze_user("disabled", status=0),
        _bronze_user("other", status=4),
        _bronze_user("no_status", status=None),
    )

    assert [r.sys_id for r in result.collect()] == ["active"]


def test_users_keeps_corrupt_rows_flagged(spark):
    """An unparseable JSON line has every column null (status too): it must not vanish."""
    corrupt = (
        None,
        None,
        None,
        None,
        None,
        '{"sys_id": "u9", "email": ',
        "users/part-0.json",
        SNAPSHOT_DATE,
        INGEST_TS,
    )
    result = _users(spark, _bronze_user("ok"), corrupt)

    assert {r.sys_id: r.is_corrupt for r in result.collect()} == {"ok": False, None: True}


def test_users_latest_snapshot_only(spark):
    result = _users(
        spark,
        _bronze_user("u1", email="old@corp.example", batch_date=date(2025, 1, 1)),
        _bronze_user("u1", email="new@corp.example"),
    )

    assert [r.email for r in result.collect()] == ["new@corp.example"]


def test_users_schema(spark):
    types = dict(_users(spark, _bronze_user()).dtypes)

    assert list(types) == [
        "sys_id",
        "email_raw",
        "email",
        "department",
        "site",
        "status",
        "_corrupt_record",
        "is_corrupt",
        "source_file",
        "batch_date",
        "ingest_ts",
    ]
    assert types["email"] == "string"
    assert types["is_corrupt"] == "boolean"


# --- vpn_rules -----------------------------------------------------------------------------------

BRONZE_RULES = (
    "sys_id string, app_sys_id string, gateway string, resource string, port string, "
    "protocol string, ad_group_name string, ad_group_domain string, operational_status int, "
    "_corrupt_record string, source_file string, batch_date date, ingest_ts timestamp"
)


def _bronze_rule(
    sys_id="r1",
    port="8000-8100",
    protocol=" tcp ",
    status=C.OPERATIONAL_STATUS_ACTIVE,
    corrupt=None,
    batch_date=SNAPSHOT_DATE,
    resource="srv-db-0001",
):
    """One bronze vpn_rules row with valid defaults; override only what a test is about."""
    return (
        sys_id,
        "app1",
        "gw-01",
        resource,
        port,
        protocol,
        "grp-0001",
        "corp.example",
        status,
        corrupt,
        "vpn_rules/part-0.json",
        batch_date,
        INGEST_TS,
    )


def _rules(spark, *rows):
    return vpn_rules(spark.createDataFrame(list(rows), BRONZE_RULES))


def test_vpn_rules_ports_and_protocol_parsed_raw_kept(spark):
    row = _rules(spark, _bronze_rule()).first()

    assert (row.port_start, row.port_end, row.port_spec_valid) == (8000, 8100, True)
    assert row.port_raw == "8000-8100"
    assert row.protocol == "TCP"
    assert row.protocol_raw == " tcp "
    assert row.is_corrupt is False


def test_vpn_rules_invalid_port_flagged_not_dropped(spark):
    result = _rules(spark, _bronze_rule("ok"), _bronze_rule("bad", port="99-1"))

    flags = {r.sys_id: (r.port_start, r.port_spec_valid) for r in result.collect()}
    assert flags == {"ok": (8000, True), "bad": (None, False)}


def test_vpn_rules_keeps_operational_only(spark):
    result = _rules(
        spark,
        _bronze_rule("on", status=C.OPERATIONAL_STATUS_ACTIVE),
        _bronze_rule("off", status=0),
        _bronze_rule("suspended", status=3),
    )

    assert [r.sys_id for r in result.collect()] == ["on"]


def test_vpn_rules_keeps_corrupt_rows_flagged(spark):
    corrupt = (None,) * 9 + ("{not json", "vpn_rules/part-0.json", SNAPSHOT_DATE, INGEST_TS)
    result = _rules(spark, _bronze_rule("ok"), corrupt)

    assert {r.sys_id: r.is_corrupt for r in result.collect()} == {"ok": False, None: True}


def test_vpn_rules_latest_snapshot_only(spark):
    result = _rules(
        spark,
        _bronze_rule("r1", port="22", batch_date=date(2025, 1, 1)),
        _bronze_rule("r1", port="443"),
    )

    assert [r.port_raw for r in result.collect()] == ["443"]


def test_vpn_rules_schema(spark):
    types = dict(_rules(spark, _bronze_rule()).dtypes)

    assert "port" not in types  # renamed to port_raw
    assert types["port_raw"] == "string"
    assert types["port_start"] == "int"
    assert types["port_end"] == "int"
    assert types["port_spec_valid"] == "boolean"
    assert types["protocol"] == "string"
    assert types["is_corrupt"] == "boolean"


# --- users and vpn_rules on generated TINY data --------------------------------------------------


def _tiny_bronze(spark, tiny_landing, entity):
    """Landing snapshot + bronze metadata, as the bronze job would store it (no Delta write)."""
    landing = io.read_landing(spark, tiny_landing.root, entity)
    return add_ingest_metadata(landing, INGEST_TS, batch_date=tiny_landing.end)


def test_users_on_generated_data(spark, tiny_landing):
    bronze = _tiny_bronze(spark, tiny_landing, "users")
    silver = users(bronze).cache()

    assert silver.count() == bronze.where(F.col("status") == C.USER_STATUS_ACTIVE).count()
    assert silver.where(F.col("email").isNull() | F.col("is_corrupt")).count() == 0
    assert silver.select("email").distinct().count() == silver.count()  # emails stay unique
    silver.unpersist()


def test_vpn_rules_on_generated_data(spark, tiny_landing):
    bronze = _tiny_bronze(spark, tiny_landing, "vpn_rules")
    silver = vpn_rules(bronze).cache()

    operational = bronze.where(F.col("operational_status") == C.OPERATIONAL_STATUS_ACTIVE)
    assert silver.count() == operational.count()
    assert silver.where(~F.col("port_spec_valid") | F.col("is_corrupt")).count() == 0
    assert {r.protocol for r in silver.select("protocol").distinct().collect()} <= {"TCP", "UDP"}
    silver.unpersist()


# --- apps and ad_groups --------------------------------------------------------------------------

BRONZE_APPS = (
    "sys_id string, name string, department string, "
    "_corrupt_record string, source_file string, batch_date date, ingest_ts timestamp"
)
BRONZE_GROUPS = (
    "group_id string, name string, domain string, "
    "_corrupt_record string, source_file string, batch_date date, ingest_ts timestamp"
)


def _bronze_snapshot_row(key, a, b, corrupt=None, batch_date=SNAPSHOT_DATE):
    """One bronze row of a 3-column snapshot entity (apps or ad_group) plus metadata."""
    return (key, a, b, corrupt, "part-0", batch_date, INGEST_TS)


def test_apps_latest_snapshot_and_corrupt_flag(spark):
    bronze = spark.createDataFrame(
        [
            _bronze_snapshot_row("a1", "app-old", "IT", batch_date=date(2025, 1, 1)),
            _bronze_snapshot_row("a1", "app-new", "IT"),
            _bronze_snapshot_row(None, None, None, corrupt="{broken"),
        ],
        BRONZE_APPS,
    )

    rows = {(r.sys_id, r.name, r.is_corrupt) for r in apps(bronze).collect()}

    assert rows == {("a1", "app-new", False), (None, None, True)}


def test_apps_schema(spark):
    bronze = spark.createDataFrame([_bronze_snapshot_row("a1", "app", "IT")], BRONZE_APPS)

    assert apps(bronze).columns == [
        "sys_id",
        "name",
        "department",
        "_corrupt_record",
        "is_corrupt",
        "source_file",
        "batch_date",
        "ingest_ts",
    ]


def test_ad_groups_latest_snapshot_and_corrupt_flag(spark):
    bronze = spark.createDataFrame(
        [
            _bronze_snapshot_row("g1", "grp-old", "corp.example", batch_date=date(2025, 1, 1)),
            _bronze_snapshot_row("g1", "grp-new", "corp.example"),
            _bronze_snapshot_row("g2", "grp-x", "corp.example", corrupt="g2,grp-x,corp,EXTRA"),
        ],
        BRONZE_GROUPS,
    )

    rows = {(r.group_id, r.name, r.is_corrupt) for r in ad_groups(bronze).collect()}

    assert rows == {("g1", "grp-new", False), ("g2", "grp-x", True)}


def test_ad_groups_names_are_not_normalized(spark):
    """Rules match groups by the exact (name, domain) they write; silver must not change it."""
    bronze = spark.createDataFrame(
        [_bronze_snapshot_row("g1", " Grp-Mixed ", "EU.corp.example")], BRONZE_GROUPS
    )

    row = ad_groups(bronze).first()

    assert (row.name, row.domain) == (" Grp-Mixed ", "EU.corp.example")


def test_ad_groups_schema(spark):
    bronze = spark.createDataFrame(
        [_bronze_snapshot_row("g1", "grp", "corp.example")], BRONZE_GROUPS
    )

    assert ad_groups(bronze).columns == [
        "group_id",
        "name",
        "domain",
        "_corrupt_record",
        "is_corrupt",
        "source_file",
        "batch_date",
        "ingest_ts",
    ]


def test_apps_and_ad_groups_on_generated_data(spark, tiny_landing):
    silver_apps = apps(_tiny_bronze(spark, tiny_landing, "apps"))
    groups = ad_groups(_tiny_bronze(spark, tiny_landing, "ad_group")).cache()

    assert silver_apps.count() == C.TINY.apps
    assert silver_apps.select("sys_id").distinct().count() == C.TINY.apps

    total = groups.count()
    assert total == C.TINY.ad_groups
    assert groups.where("is_corrupt").count() == 0
    # The pair is the key rules use: unique...
    assert groups.select("name", "domain").distinct().count() == total
    # ...while the name alone is not, which is why rules carry the domain too.
    assert groups.select("name").distinct().count() < total
    groups.unpersist()


# --- group_members -------------------------------------------------------------------------------

BRONZE_MEMBERSHIPS = (
    "group_id string, member_id string, group_domain string, member_type string, "
    "_corrupt_record string, source_file string, batch_date date, ingest_ts timestamp"
)


def _bronze_member(
    group_id="g1",
    member_id="u1",
    member_type="user",
    corrupt=None,
    batch_date=SNAPSHOT_DATE,
    source="ad_group_membership/part-0.csv",
):
    """One bronze membership row with valid defaults; override only what a test is about."""
    return (
        group_id,
        member_id,
        "corp.example",
        member_type,
        corrupt,
        source,
        batch_date,
        INGEST_TS,
    )


def _members(spark, *rows, groups=("g1", "g2"), users=("u1", "u2")):
    """Run group_members with small lookup tables: known group ids and known user sys_ids."""
    memberships = spark.createDataFrame(list(rows), BRONZE_MEMBERSHIPS)
    known_groups = spark.createDataFrame([(g,) for g in groups], "group_id string")
    known_users = spark.createDataFrame([(u,) for u in users], "sys_id string")
    return group_members(memberships, known_groups, known_users)


def _pairs(df):
    """Collect as {(group_id, member_id): (is_corrupt, is_orphan)}."""
    return {(r.group_id, r.member_id): (r.is_corrupt, r.is_orphan) for r in df.collect()}


def test_group_members_keeps_user_members_only(spark):
    result = _members(
        spark,
        _bronze_member("g1", "u1", member_type="user"),
        _bronze_member("g1", "cmp1", member_type="computer"),
        _bronze_member("g1", "u2", member_type=None),
    )

    assert _pairs(result) == {("g1", "u1"): (False, False)}


def test_group_members_removes_exact_duplicates(spark):
    result = _members(
        spark,
        _bronze_member("g1", "u1"),
        _bronze_member("g1", "u1"),
        _bronze_member("g1", "u1", source="ad_group_membership/part-1.csv"),  # other file
        _bronze_member("g2", "u1"),
    )

    assert result.count() == 2
    assert set(_pairs(result)) == {("g1", "u1"), ("g2", "u1")}


def test_group_members_corrupt_row_does_not_replace_clean_copy(spark):
    """Same (group, member) once clean and once corrupt: both stay, only one is flagged."""
    result = _members(
        spark,
        _bronze_member("g1", "u1"),
        _bronze_member("g1", "u1", corrupt="g1,u1,corp.example,user,EXTRA"),
    )

    flags = sorted(r.is_corrupt for r in result.collect())
    assert flags == [False, True]


def test_group_members_keeps_corrupt_rows_with_null_columns(spark):
    """Two different unparseable lines: all-null columns, but neither is dropped or merged."""
    result = _members(
        spark,
        _bronze_member(None, None, member_type=None, corrupt="broken line 1"),
        _bronze_member(None, None, member_type=None, corrupt="broken line 2"),
    )

    assert result.count() == 2
    assert all(r.is_corrupt for r in result.collect())


def test_group_members_flags_orphans_without_dropping_them(spark):
    result = _members(
        spark,
        _bronze_member("g1", "u1"),  # both known
        _bronze_member("grp-missing", "u1"),  # unknown group
        _bronze_member("g1", "usr-missing"),  # unknown member
        _bronze_member("grp-missing", "usr-missing"),  # both unknown
    )

    assert _pairs(result) == {
        ("g1", "u1"): (False, False),
        ("grp-missing", "u1"): (False, True),
        ("g1", "usr-missing"): (False, True),
        ("grp-missing", "usr-missing"): (False, True),
    }


def test_group_members_duplicate_lookup_keys_do_not_multiply_rows(spark):
    """A key twice in a lookup table must flag the membership, not duplicate it (fan-out)."""
    result = _members(spark, _bronze_member("g1", "u1"), groups=("g1", "g1"), users=("u1", "u1"))

    assert result.count() == 1


def test_group_members_latest_snapshot_only(spark):
    result = _members(
        spark,
        _bronze_member("g1", "u1", batch_date=date(2025, 1, 1)),
        _bronze_member("g2", "u2"),
    )

    assert set(_pairs(result)) == {("g2", "u2")}


def test_group_members_schema(spark):
    result = _members(spark, _bronze_member())

    assert result.columns == [
        "group_id",
        "member_id",
        "group_domain",
        "member_type",
        "_corrupt_record",
        "is_corrupt",
        "is_orphan",
        "source_file",
        "batch_date",
        "ingest_ts",
    ]
    assert dict(result.dtypes)["is_orphan"] == "boolean"


def test_group_members_on_generated_data(spark, tiny_landing):
    memberships = _tiny_bronze(spark, tiny_landing, "ad_group_membership")
    groups = ad_groups(_tiny_bronze(spark, tiny_landing, "ad_group"))
    all_users = latest_snapshot(_tiny_bronze(spark, tiny_landing, "users"))  # any status
    silver = group_members(memberships, groups, all_users).cache()

    rows = silver.count()
    clean = silver.where(~F.col("is_corrupt"))
    # No duplicate pair is left among clean rows, and only user members remain.
    assert clean.select("group_id", "member_id").distinct().count() == clean.count()
    assert silver.where(F.col("member_type") != C.MEMBER_TYPE_USER).count() == 0
    # The generator plants malformed rows; they survive, flagged.
    assert silver.where("is_corrupt").count() > 0
    # Planted orphans are flagged: unknown groups and members carry a "-missing-" id.
    planted = silver.where(
        F.col("group_id").contains("-missing-") | F.col("member_id").contains("-missing-")
    )
    assert planted.count() > 0
    assert planted.where(~F.col("is_orphan")).count() == 0
    assert silver.where("is_orphan").count() == planted.count()  # and nothing else is
    assert rows < memberships.count()  # non-user members and duplicates removed
    silver.unpersist()


# --- resolution_states ---------------------------------------------------------------------------

BRONZE_RESOLUTIONS = (
    "resource_name string, state_id string, ip_start bigint, ip_end bigint, "
    "first_seen timestamp, last_seen timestamp, source_file string, batch_date date, "
    "ingest_ts timestamp"
)
JAN_1 = datetime(2025, 1, 1, tzinfo=UTC)
JAN_2 = datetime(2025, 1, 2, tzinfo=UTC)


def _bronze_state(
    state_id="s1",
    ip_start=167772161,  # 10.0.0.1
    ip_end=167772161,
    first_seen=JAN_1,
    last_seen=JAN_2,
    batch_date=SNAPSHOT_DATE,
    resource="srv-db-0001",
):
    """One bronze resolution row with valid defaults; override only what a test is about."""
    return (
        resource,
        state_id,
        ip_start,
        ip_end,
        first_seen,
        last_seen,
        "resource_resolution/part-0.parquet",
        batch_date,
        INGEST_TS,
    )


def _states(spark, *rows):
    return resolution_states(spark.createDataFrame(list(rows), BRONZE_RESOLUTIONS))


def _flags(df, column):
    return {r.state_id: r[column] for r in df.collect()}


def test_resolution_states_valid_rows(spark):
    result = _states(
        spark,
        _bronze_state("single_ip"),
        _bronze_state("subnet", ip_start=167772160, ip_end=167772415),  # 10.0.0.0/24
        _bronze_state("current", last_seen=None),  # NULL last_seen = current state
        _bronze_state("whole_space", ip_start=0, ip_end=IPV4_MAX),
    )

    assert all(r.ip_range_valid and r.period_valid for r in result.collect())
    assert result.count() == 4


def test_resolution_states_invalid_ip_ranges_flagged_not_dropped(spark):
    result = _states(
        spark,
        _bronze_state("ok"),
        _bronze_state("reversed", ip_start=20, ip_end=10),
        _bronze_state("negative", ip_start=-1, ip_end=10),
        _bronze_state("above_ipv4", ip_start=10, ip_end=IPV4_MAX + 1),
        _bronze_state("no_start", ip_start=None),
        _bronze_state("no_end", ip_end=None),
    )

    assert _flags(result, "ip_range_valid") == {
        "ok": True,
        "reversed": False,
        "negative": False,
        "above_ipv4": False,
        "no_start": False,  # null, not None: coalesce keeps the flag strictly boolean
        "no_end": False,
    }


def test_resolution_states_invalid_periods_flagged_not_dropped(spark):
    result = _states(
        spark,
        _bronze_state("ok"),
        _bronze_state("current", last_seen=None),
        _bronze_state("empty", first_seen=JAN_1, last_seen=JAN_1),  # [t, t) contains nothing
        _bronze_state("reversed", first_seen=JAN_2, last_seen=JAN_1),
        _bronze_state("no_start", first_seen=None),
    )

    assert _flags(result, "period_valid") == {
        "ok": True,
        "current": True,
        "empty": False,
        "reversed": False,
        "no_start": False,
    }


def test_resolution_states_latest_snapshot_only(spark):
    result = _states(
        spark,
        _bronze_state("old", batch_date=date(2025, 1, 1)),
        _bronze_state("new"),
    )

    assert [r.state_id for r in result.collect()] == ["new"]


def test_resolution_states_schema(spark):
    result = _states(spark, _bronze_state())

    assert result.columns == [
        "resource_name",
        "state_id",
        "ip_start",
        "ip_end",
        "first_seen",
        "last_seen",
        "ip_range_valid",
        "period_valid",
        "source_file",
        "batch_date",
        "ingest_ts",
    ]
    types = dict(result.dtypes)
    assert (types["ip_start"], types["ip_end"]) == ("bigint", "bigint")
    assert (types["ip_range_valid"], types["period_valid"]) == ("boolean", "boolean")


def test_resolution_states_on_generated_data(spark, tiny_landing):
    bronze = _tiny_bronze(spark, tiny_landing, "resource_resolution")
    silver = resolution_states(bronze).cache()

    assert silver.count() == bronze.count()
    assert silver.where(~F.col("ip_range_valid") | ~F.col("period_valid")).count() == 0

    # What the matching join relies on, per resource (one row per state, not per IP range):
    states = silver.select("resource_name", "state_id", "first_seen", "last_seen").distinct()
    # 1. Exactly one current state.
    current = states.groupBy("resource_name").agg(
        F.sum(F.col("last_seen").isNull().cast("int")).alias("n_current")
    )
    assert current.where(F.col("n_current") != 1).count() == 0
    # 2. States do not overlap: each one starts at or after the previous one ended.
    #    partitionBy(resource_name): each resource is ordered on its own, in parallel.
    by_resource = Window.partitionBy("resource_name").orderBy("first_seen")
    ordered = states.withColumn("prev_last_seen", F.lag("last_seen").over(by_resource))
    assert ordered.where(F.col("first_seen") < F.col("prev_last_seen")).count() == 0
    silver.unpersist()


# --- rule_access ---------------------------------------------------------------------------------


def _access(spark, rule_rows, state_rows):
    """Build silver rules and states from bronze rows, then rule_access."""
    rules = _rules(spark, *rule_rows)
    states = (
        _states(spark, *state_rows)
        if state_rows
        else resolution_states(spark.createDataFrame([], BRONZE_RESOLUTIONS))
    )
    return rule_access(rules, states)


def test_rule_access_one_row_per_state_and_range(spark):
    """A rule gets every (state, range) of its resource: 2 states, one with 2 ranges -> 3 rows."""
    result = _access(
        spark,
        [_bronze_rule("r1")],
        [
            _bronze_state("s1", ip_start=10, ip_end=10),
            _bronze_state("s1", ip_start=20, ip_end=20),  # same state, second IP range
            _bronze_state("s2", ip_start=30, ip_end=30, first_seen=JAN_2, last_seen=None),
            _bronze_state("other", resource="srv-web-0002"),  # another resource: not joined
        ],
    )

    rows = sorted((r.state_id, r.ip_start) for r in result.collect())
    assert rows == [("s1", 10), ("s1", 20), ("s2", 30)]
    assert all(r.rule_sys_id == "r1" and r.has_resolution for r in result.collect())


def test_rule_access_every_rule_on_a_resource_gets_its_states(spark):
    """Two rules on one resource: the states are repeated per rule (fan-out is intended here)."""
    result = _access(
        spark,
        [_bronze_rule("r1"), _bronze_rule("r2", port="443")],
        [_bronze_state("s1", ip_start=10, ip_end=10), _bronze_state("s1", ip_start=20, ip_end=20)],
    )

    assert result.count() == 4
    assert {r.rule_sys_id for r in result.collect()} == {"r1", "r2"}


def test_rule_access_rule_without_resolution_is_kept_and_flagged(spark):
    result = _access(
        spark,
        [_bronze_rule("r1"), _bronze_rule("lonely", resource="srv-missing-0001")],
        [_bronze_state("s1")],
    )

    lonely = result.where(F.col("rule_sys_id") == "lonely").collect()
    assert len(lonely) == 1
    row = lonely[0]
    assert row.has_resolution is False
    assert (row.state_id, row.ip_start, row.first_seen) == (None, None, None)
    # Strictly boolean: False, not None (where(~flag) must find these rows).
    assert (row.ip_range_valid, row.period_valid) == (False, False)


def test_rule_access_no_states_at_all(spark):
    result = _access(spark, [_bronze_rule("r1")], [])

    assert [(r.rule_sys_id, r.has_resolution) for r in result.collect()] == [("r1", False)]


def test_rule_access_carries_invalid_flags_without_dropping(spark):
    result = _access(
        spark,
        [_bronze_rule("bad_port", port="99-1")],
        [
            _bronze_state("ok"),
            _bronze_state("bad_range", ip_start=20, ip_end=10),
            _bronze_state("bad_period", first_seen=JAN_2, last_seen=JAN_1),
        ],
    )

    flags = {
        r.state_id: (r.port_spec_valid, r.ip_range_valid, r.period_valid) for r in result.collect()
    }
    assert flags == {
        "ok": (False, True, True),
        "bad_range": (False, False, True),
        "bad_period": (False, True, False),
    }


def test_rule_access_keeps_corrupt_rules(spark):
    """A corrupt rule has a null resource: it matches no state but keeps its row, flagged."""
    corrupt = (None,) * 9 + ("{not json", "vpn_rules/part-0.json", SNAPSHOT_DATE, INGEST_TS)
    result = _access(spark, [_bronze_rule("r1"), corrupt], [_bronze_state("s1")])

    rows = {(r.rule_sys_id, r.is_corrupt, r.has_resolution) for r in result.collect()}
    assert rows == {("r1", False, True), (None, True, False)}


def test_rule_access_schema(spark):
    result = _access(spark, [_bronze_rule()], [_bronze_state()])

    assert result.columns == [
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
        "has_resolution",
        "port_spec_valid",
        "ip_range_valid",
        "period_valid",
        "is_corrupt",
    ]
    types = dict(result.dtypes)
    assert (types["ip_start"], types["port_start"]) == ("bigint", "int")
    assert types["has_resolution"] == "boolean"


def test_rule_access_on_generated_data(spark, tiny_landing):
    rules = vpn_rules(_tiny_bronze(spark, tiny_landing, "vpn_rules")).cache()
    states = resolution_states(_tiny_bronze(spark, tiny_landing, "resource_resolution")).cache()
    access = rule_access(rules, states).cache()

    # Every rule is there, and every generated rule's resource has resolutions.
    assert access.select("rule_sys_id").distinct().count() == rules.count()
    assert access.where(~F.col("has_resolution")).count() == 0
    # Row count = for each rule, the number of state rows of its resource.
    rows_per_resource = states.groupBy("resource_name").count()
    expected = (
        rules.join(rows_per_resource, rules["resource"] == rows_per_resource["resource_name"])
        .agg(F.sum("count"))
        .first()[0]
    )
    assert access.count() == expected
    # Generated data is clean: every row is usable by matching.
    usable = F.col("port_spec_valid") & F.col("ip_range_valid") & F.col("period_valid")
    assert access.where(~usable).count() == 0
    for df in (access, states, rules):
        df.unpersist()
