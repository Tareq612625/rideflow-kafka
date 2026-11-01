"""The tools around Kafka. These need: docker compose --profile full up -d"""

import asyncio
import io
import json
import struct
import time
import fastavro
import httpx
from aiokafka.structs import TopicPartition
from app import config, kit
from app.kit import Report, consumer, producer, read, topic
from app.registry import feature

GROUP = "Ecosystem"
NEEDS_FULL = "Start it with: docker compose --profile full up -d"


async def reachable(http: httpx.AsyncClient, path: str, name: str) -> None:
    try:
        await http.get(path)
    except httpx.HTTPError as exc:
        raise kit.Skip(f"{name} is not running. {NEEDS_FULL}") from exc


RIDE_V1 = {
    "type": "record", "name": "Ride", "namespace": "rideflow",
    "fields": [{"name": "trip_id", "type": "string"}, {"name": "fare", "type": "double"}],
}
RIDE_V2 = {
    **RIDE_V1,
    "fields": RIDE_V1["fields"] + [{"name": "currency", "type": "string", "default": "BDT"}],
}
RIDE_BAD = {
    **RIDE_V1,
    "fields": RIDE_V1["fields"] + [{"name": "tip", "type": "double"}],   # no default
}


def encode(schema_id: int, schema: dict, record: dict) -> bytes:
    """The standard wire format: magic byte 0, 4-byte schema id, then Avro bytes."""
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, fastavro.parse_schema(schema), record)
    return b"\x00" + struct.pack(">I", schema_id) + buffer.getvalue()


@feature(
    "schema-registry", GROUP, "Schema Registry",
    "The registry stores the agreed shape of your data, and refuses a change that would break readers.",
    "Kafka only sees bytes. This is what stops one team's change from crashing every consumer.",
    'POST /subjects/rides-value/versions   {"schema": "..."}',
    optional=True,
)
async def schema_registry(r: Report) -> None:
    subject = f"{kit.unique('rides')}-value"
    async with httpx.AsyncClient(base_url=config.SCHEMA_REGISTRY_URL, timeout=15) as http:
        await reachable(http, "/subjects", "Schema Registry")

        async def register(schema: dict) -> httpx.Response:
            return await http.post(f"/subjects/{subject}/versions", json={"schema": json.dumps(schema)})

        first = await register(RIDE_V1)
        v1_id = first.json()["id"]
        r.step(f"Registered version 1 of the ride schema, as schema id {v1_id}.")

        second = await register(RIDE_V2)
        v2_id = second.json().get("id")
        r.check("Adding a field with a default was accepted", second.status_code == 200, f"schema id {v2_id}")

        third = await register(RIDE_BAD)
        r.check("Adding a required field was refused", third.status_code == 409, third.status_code)

        async with topic("avro") as name, producer(acks="all") as p:
            await p.send_and_wait(name, value=encode(v1_id, RIDE_V1, {"trip_id": "TRIP-1", "fare": 335.75}))
            async with consumer() as c:
                c.assign([TopicPartition(name, 0)])
                await c.seek_to_beginning()
                message = (await read(c, want=1))[0]

            raw = message.value
            magic, schema_id = raw[0], struct.unpack(">I", raw[1:5])[0]
            writer_schema = json.loads((await http.get(f"/schemas/ids/{schema_id}")).json()["schema"])
            decoded = fastavro.schemaless_reader(
                io.BytesIO(raw[5:]),
                fastavro.parse_schema(writer_schema),
                fastavro.parse_schema(RIDE_V2),
            )
            r.table(
                ["Measure", "Value"],
                [["Bytes in Kafka", len(raw)], ["Magic byte", magic], ["Schema id in the message", schema_id],
                 ["Decoded with the new schema", decoded]],
            )
            r.check("Old data was read with the new schema", decoded["currency"] == "BDT" and decoded["fare"] == 335.75, decoded)


@feature(
    "connect", GROUP, "Kafka Connect",
    "Connect moves data between Kafka and other systems with no custom code. This sink writes a topic to a file.",
    "Ready-made connectors exist for most databases and cloud stores.",
    'POST /connectors\n{"connector.class": "FileStreamSinkConnector",\n "topics": "orders", "file": "orders.txt"}',
    optional=True,
)
async def connect(r: Report) -> None:
    async with httpx.AsyncClient(base_url=config.CONNECT_URL, timeout=20) as http:
        await reachable(http, "/connectors", "Kafka Connect")
        worker = (await http.get("/")).json()
        r.step(f"Connect worker version {worker.get('version')} is running.")

        name = kit.unique("sink").replace(".", "-")
        file_name = f"{name}.txt"
        async with topic("connect") as source, producer(acks="all") as p:
            created = await http.post("/connectors", json={
                "name": name,
                "config": {
                    "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
                    "tasks.max": "1",
                    "topics": source,
                    "file": f"/connect-data/{file_name}",
                    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
                    "consumer.override.auto.offset.reset": "earliest",
                },
            })
            r.check("The connector was created through the REST API", created.status_code == 201, created.status_code)

            try:
                async def running():
                    status = (await http.get(f"/connectors/{name}/status")).json()
                    tasks = status.get("tasks") or []
                    return bool(tasks) and tasks[0].get("state") == "RUNNING"
                await kit.wait_until(running, timeout=30, every=0.5, what="the connector")

                for i in range(5):
                    await p.send_and_wait(source, value=f"invoice line {i}".encode())

                path = config.CONNECT_DATA / file_name
                def lines():
                    return path.read_text().splitlines() if path.exists() else []
                await kit.wait_until(lambda: len(lines()) >= 5, timeout=30, every=0.5, what="the file sink")
                r.table(["Line in the file Connect wrote"], [[line] for line in lines()])
                r.check("Connect copied all 5 messages to the file", lines() == [f"invoice line {i}" for i in range(5)])
            finally:
                await http.delete(f"/connectors/{name}")


@feature(
    "mirror-maker", GROUP, "MirrorMaker 2",
    "MirrorMaker copies topics to a second cluster. Here it copies lab.mirror-demo to a disaster recovery cluster.",
    "If a whole site is lost, the other cluster already has the data.",
    "primary->dr.enabled = true\nprimary->dr.topics = lab\\.mirror.*",
    optional=True,
)
async def mirror_maker(r: Report) -> None:
    probe = httpx.AsyncClient()
    await probe.aclose()
    source, mirrored = "lab.mirror-demo", "primary.lab.mirror-demo"
    try:
        reader = kit.consumer(bootstrap_servers=config.DR_BOOTSTRAP, request_timeout_ms=5000)
        c = await asyncio.wait_for(reader.__aenter__(), 10)
    except Exception as exc:
        raise kit.Skip(f"The second cluster is not running. {NEEDS_FULL}") from exc

    try:
        marker = kit.unique("mirror").encode()
        async with producer(acks="all") as p:
            await p.send_and_wait(source, key=b"mirror", value=marker)
        r.step(f"Wrote one message to {source} on the main cluster.")

        tp = TopicPartition(mirrored, 0)
        started = time.monotonic()

        async def arrived():
            if mirrored not in await c.topics():
                await c._client.force_metadata_update()
                return False
            c.assign([tp])
            await c.seek_to_beginning(tp)
            return any(m.value == marker for m in await read(c, quiet_ms=500, limit=4))

        await kit.wait_until(arrived, timeout=60, every=1.0, what="the mirrored message")
        r.check("The message reached the second cluster", True, f"after {time.monotonic() - started:.1f} s")
        r.step(f"It is stored there as {mirrored}. The name shows which cluster it came from.")
    finally:
        await reader.__aexit__(None, None, None)


@feature(
    "monitoring", GROUP, "Monitoring with Prometheus and Grafana",
    "An exporter publishes Kafka numbers. Prometheus stores them, and Grafana draws the dashboard on port 3000.",
    "Dashboards are for looking. Alerts on these same numbers are what wake someone up.",
    "kafka_consumergroup_lag\nkafka_topic_partition_under_replicated_partition",
    optional=True,
)
async def monitoring(r: Report) -> None:
    async with httpx.AsyncClient(base_url=config.PROMETHEUS_URL, timeout=15) as http:
        await reachable(http, "/-/ready", "Prometheus")

        async def query(expression: str) -> list:
            return (await http.get("/api/v1/query", params={"query": expression})).json()["data"]["result"]

        async def scraped():
            return await query("kafka_brokers")
        brokers = await kit.wait_until(scraped, timeout=45, every=1.0, what="the first scrape")
        count = float(brokers[0]["value"][1])
        lag = await query('sum by (consumergroup) (kafka_consumergroup_lag{consumergroup=~"analytics|billing"})')
        under = await query("sum(kafka_topic_partition_under_replicated_partition)")

        r.table(
            ["Metric", "Value right now"],
            [["Brokers", int(count)],
             ["Under-replicated partitions", under[0]["value"][1] if under else "0"]]
            + [[f'Lag of group {item["metric"]["consumergroup"]}', item["value"][1]] for item in lag],
            "Live numbers read from Prometheus",
        )
        r.check("Prometheus sees all 3 brokers", count == 3, count)

    async with httpx.AsyncClient(base_url=config.GRAFANA_URL, timeout=15) as http:
        await reachable(http, "/api/health", "Grafana")
        boards = (await http.get("/api/search", params={"query": "RideFlow"})).json()
        titles = [b["title"] for b in boards]
        r.check("Grafana has the RideFlow dashboard ready", "RideFlow Kafka" in titles, titles)
