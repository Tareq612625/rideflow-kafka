"""Settings for the Feature Lab. Override any of these with env vars."""

import os
from pathlib import Path

SERVICE_NAME = "lab-service"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SASL_BOOTSTRAP = os.getenv("KAFKA_SASL_SERVERS", "kafka-1:29093,kafka-2:29093,kafka-3:29093")
TLS_BOOTSTRAP = os.getenv("KAFKA_TLS_SERVERS", "kafka-1:29094,kafka-2:29094,kafka-3:29094")

BROKER_DATA = Path(os.getenv("BROKER_DATA", "/broker-data"))
CA_FILE = os.getenv("CA_FILE", "/certs/ca.pem")

# only present with: docker compose --profile full up
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
CONNECT_URL = os.getenv("CONNECT_URL", "http://kafka-connect:8083")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
GRAFANA_URL = os.getenv("GRAFANA_URL", "http://grafana:3000")
DR_BOOTSTRAP = os.getenv("KAFKA_DR_BOOTSTRAP", "kafka-dr:29092")
CONNECT_DATA = Path(os.getenv("CONNECT_DATA", "/connect-data"))

RUN_TIMEOUT_SEC = float(os.getenv("RUN_TIMEOUT_SEC", "120"))
CLUSTER_REFRESH_SEC = float(os.getenv("CLUSTER_REFRESH_SEC", "1.5"))
CLUSTER_STALE_SEC = float(os.getenv("CLUSTER_STALE_SEC", "15"))

STATIC_DIR = Path(__file__).parent / "static"
