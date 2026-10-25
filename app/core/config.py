"""Single source of truth for every tunable in the bulk processor.

The upstream Hospital Directory API advertises no rate limits, so the
client-side limits below are deliberately conservative: bounded concurrency
plus retry-with-backoff keeps us polite to the shared deployment while still
finishing a 20-row batch in seconds.
"""
import os

# --- Application ---
APP_VERSION = "1.0.0"
API_PREFIX = "/hospitals/bulk"

# --- Upstream Hospital Directory API ---
HOSPITAL_API_BASE_URL = os.getenv(
    "HOSPITAL_API_BASE_URL", "https://hospital-directory.onrender.com"
)
# Render free tier can cold-start slowly; generous per-request timeout.
UPSTREAM_TIMEOUT_SECONDS = float(os.getenv("UPSTREAM_TIMEOUT_SECONDS", "30"))

# --- Client-side rate limiting (protects the upstream API) ---
# Global cap on in-flight upstream calls, shared across all batches.
MAX_CONCURRENT_UPSTREAM_CALLS = int(os.getenv("MAX_CONCURRENT_UPSTREAM_CALLS", "5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))  # retries after the first attempt
BACKOFF_BASE_SECONDS = 0.5   # delay grows base * 2^attempt, plus jitter
BACKOFF_CAP_SECONDS = 8.0
# Transient upstream statuses worth retrying; any other 4xx is a permanent
# failure for that row. A Retry-After header, when present, overrides backoff.
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

# --- CSV validation ---
MAX_CSV_ROWS = 20            # hard limit from the assignment spec
MAX_UPLOAD_BYTES = 256 * 1024
CSV_REQUIRED_COLUMNS = ("name", "address")
CSV_OPTIONAL_COLUMNS = ("phone",)
MAX_FIELD_LENGTHS = {"name": 200, "address": 300, "phone": 32}

# --- Server-sent events ---
SSE_HEARTBEAT_SECONDS = 15.0  # comment line cadence while a batch is quiet
