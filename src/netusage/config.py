"""Central configuration: scale, time range, dirty-data ratios, shapes, business rules, locations.

Every tunable number in the project lives here. The generator and pipeline import from this
module instead of hard-coding values.
"""

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path


# --- Scale ------------------------------------------------------------------------------------
# A "profile" bundles all sizes, so switching between a quick run and the full run is one choice.
@dataclass(frozen=True)
class ScaleProfile:
    name: str
    users: int
    apps: int
    ad_groups: int
    avg_members_per_group: int  # total memberships ≈ ad_groups × this
    vpn_rules: int
    resources: int
    traffic_events: int  # total over the whole date range


SMALL = ScaleProfile(
    name="small",
    users=10_000,
    apps=200,
    ad_groups=50_000,
    avg_members_per_group=40,
    vpn_rules=12_000,
    resources=8_000,
    traffic_events=1_000_000,
)

FULL = ScaleProfile(
    name="full",
    users=100_000,
    apps=1_500,
    ad_groups=500_000,
    avg_members_per_group=40,  # ≈ 20M memberships
    vpn_rules=120_000,
    resources=80_000,
    traffic_events=100_000_000,
)

# Seconds-fast profile for the generator's integration tests (pair it with a few-day date range).
TINY = ScaleProfile(
    name="tiny",
    users=500,
    apps=20,
    ad_groups=1_000,
    avg_members_per_group=10,
    vpn_rules=600,
    resources=400,
    traffic_events=20_000,
)

PROFILES = {p.name: p for p in (TINY, SMALL, FULL)}

# Same seed -> byte-identical landing data. The generator derives every random value by hashing
# (SEED, purpose, row key), so values don't depend on how Spark partitions the work.
SEED = 42


# --- Time -------------------------------------------------------------------------------------
START_DATE = date(2025, 1, 1)
END_DATE = date(2025, 12, 31)  # inclusive; one landing batch per day
TIMEZONE = "UTC"


# --- Dirty-data ratios (share of rows affected) -----------------------------------------------
LATE_EVENT_RATIO = 0.01  # events landing in a later daily batch...
LATE_EVENT_MAX_DAYS = 3  # ...by 1 to this many days
DUPLICATE_EVENT_RATIO = 0.005  # within and across batches (retry simulation)
DENY_EVENT_RATIO = 0.05  # present in bronze, filtered out in silver
UNKNOWN_USER_EVENT_RATIO = 0.002  # emails matching no user
EMAIL_NOISE_RATIO = 0.01  # wrong case / stray whitespace in emails
DISABLED_USER_RATIO = 0.03
NON_USER_MEMBER_RATIO = 0.02  # member_type other than "user"
ORPHAN_MEMBERSHIP_RATIO = 0.001  # unknown group or member
DUPLICATE_MEMBERSHIP_RATIO = 0.001
MALFORMED_CSV_ROW_RATIO = 0.0005

# --- Derived time ------------------------------------------------------------------------------
# Landing batches run past END_DATE so late events near year-end still land in a real batch.
LANDING_END_DATE = END_DATE + timedelta(days=LATE_EVENT_MAX_DAYS)


# --- Shapes (what makes the governance metrics find something) --------------------------------
UNUSED_RULE_RATIO = 0.25  # rules no traffic will ever match
DUPLICATE_RULE_RATIO = 0.05  # same IP(s)/port/protocol/group under another app or gateway
WIDE_PORT_RANGE_RATIO = 0.03  # rules like "1-65535"
SUBNET_RESOURCE_RATIO = 0.10  # resources that are subnets rather than single IPs
SUBNET_PREFIX_LENGTHS = (24, 26, 28)  # allowed subnet sizes; /24 is the largest (256 addresses)
UNSTABLE_RESOURCE_RATIO = 0.05  # resources whose IPs change frequently
UNSTABLE_STATE_HOURS = 18  # an unstable resource gets a new resolution state this often
STABLE_STATE_DAYS = 120  # ...and a stable one this often (a few IP changes a year)
MULTI_RANGE_RESOURCE_RATIO = 0.10  # single-IP resources resolving to 2 IPs at once (DNS A records)
HUGE_GROUP_COUNT = 5  # "Domain Users"-style groups -> skew
HUGE_GROUP_MEMBER_SHARE = 0.9  # each huge group contains this share of all users
RULES_ON_HUGE_GROUP_RATIO = 0.05  # rules granted to a huge group -> user over-provisioning
RANGE_PORT_RULE_RATIO = 0.15  # rules with a moderate range like "8000-8100" (wide ones above)
UDP_RULE_RATIO = 0.10  # the rest are TCP
NON_OPERATIONAL_RULE_RATIO = 0.05  # operational_status other than OPERATIONAL


# --- Business rules ---------------------------------------------------------------------------
# users.status codes we generate: 0..6, with 1 = ACTIVE. All others are inactive users.
USER_STATUS_ACTIVE = 1
# vpn_rules.operational_status codes we generate: 0=NON-OPERATIONAL, 1=OPERATIONAL, 3=SUSPENDED.
# The pipeline checks only OPERATIONAL; every other code is excluded from the analysis.
OPERATIONAL_STATUS_ACTIVE = 1
# traffic_logs.action values: ALLOW / DENY. Silver keeps ALLOW events only.
ACTION_ALLOW = "ALLOW"
UNSTABLE_MAX_STATES = 2  # more than this many distinct resolution states...
UNSTABLE_WINDOW_DAYS = 3  # ...within this rolling window => resource is "Unstable"
# Resolutions are timestamped; the metric buckets changes by UTC date into windows
# [d - UNSTABLE_WINDOW_DAYS + 1, d] inclusive. See architecture.md §4.


# --- Locations --------------------------------------------------------------------------------
CATALOG = "netusage"
SCHEMAS = ("landing", "bronze", "silver", "gold")
LANDING_VOLUME_PATH = f"/Volumes/{CATALOG}/landing/raw"
LOCAL_DATA_PATH = Path(__file__).resolve().parents[2] / "data"
LOCAL_LANDING_PATH = LOCAL_DATA_PATH / "landing"
LOCAL_BRONZE_PATH = LOCAL_DATA_PATH / "bronze"  # one Delta folder per table (no catalog locally)
