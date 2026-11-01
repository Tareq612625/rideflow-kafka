"""Settings for the Trip Service. Override any of these with env vars."""

import os

SERVICE_NAME = "trip-service"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("RIDE_TOPIC", "ride-events")

LINGER_MS = int(os.getenv("LINGER_MS", "10"))
MAX_BATCH_SIZE = int(os.getenv("MAX_BATCH_SIZE", str(64 * 1024)))
COMPRESSION = os.getenv("COMPRESSION", "gzip")
