"""Settings for the Billing Service."""

import os
from pathlib import Path

SERVICE_NAME = "billing-service"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

SOURCE_TOPIC = os.getenv("RIDE_TOPIC", "ride-events")
INVOICE_TOPIC = os.getenv("INVOICE_TOPIC", "invoices")

DLQ_TOPIC = os.getenv("DLQ_TOPIC", "ride-events-dlq")

GROUP_ID = os.getenv("GROUP_ID", "billing")

COMMISSION_RATE = float(os.getenv("COMMISSION_RATE", "0.20"))

STATE_TOPIC = os.getenv("STATE_TOPIC", "billing-state")
CHECKPOINT_SEC = float(os.getenv("CHECKPOINT_SEC", "1.0"))

MAX_RECORDS = int(os.getenv("MAX_RECORDS", "500"))
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "250"))
LAG_REFRESH_SEC = float(os.getenv("LAG_REFRESH_SEC", "0.6"))
BROADCAST_INTERVAL_SEC = float(os.getenv("BROADCAST_INTERVAL_SEC", "0.25"))

STATIC_DIR = Path(__file__).parent / "static"
