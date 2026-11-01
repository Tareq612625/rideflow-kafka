"""Settings for the Analytics Service. Override any of these with env vars."""

import os
from pathlib import Path

SERVICE_NAME = "analytics-service"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("RIDE_TOPIC", "ride-events")

GROUP_ID = os.getenv("GROUP_ID", "analytics")

STATE_TOPIC = os.getenv("STATE_TOPIC", "analytics-state")
CHECKPOINT_SEC = float(os.getenv("CHECKPOINT_SEC", "1.0"))

MAX_RECORDS = int(os.getenv("MAX_RECORDS", "800"))
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "250"))
LAG_REFRESH_SEC = float(os.getenv("LAG_REFRESH_SEC", "0.6"))

BROADCAST_INTERVAL_SEC = float(os.getenv("BROADCAST_INTERVAL_SEC", "0.25"))
SURGE_REQUESTS_PER_3S = int(os.getenv("SURGE_REQUESTS_PER_3S", "120"))

STATIC_DIR = Path(__file__).parent / "static"
