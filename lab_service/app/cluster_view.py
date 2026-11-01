"""A live picture of the cluster, refreshed in the background for the page.

It keeps working while a broker is down, because that is the moment you most
want to watch it.
"""

import asyncio
import time
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.structs import TopicPartition
from app import config

EXPECTED_BROKERS = [1, 2, 3]


class ClusterView:
    def __init__(self) -> None:
        self.snapshot: dict = {"ok": False, "error": "starting up"}
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run_forever())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _run_forever(self) -> None:
        while True:
            admin = AIOKafkaAdminClient(bootstrap_servers=config.BOOTSTRAP, client_id="lab-panel")
            reader = AIOKafkaConsumer(bootstrap_servers=config.BOOTSTRAP, client_id="lab-panel-reader")
            try:
                await admin.start()
                await reader.start()
                while True:
                    self.snapshot = await asyncio.wait_for(self._look(admin, reader), timeout=8)
                    await asyncio.sleep(config.CLUSTER_REFRESH_SEC)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.snapshot = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
                await asyncio.sleep(2)
            finally:
                for client_stop in (admin.close, reader.stop):
                    try:
                        await asyncio.wait_for(client_stop(), timeout=5)
                    except Exception:
                        pass

    @staticmethod
    async def _look(admin: AIOKafkaAdminClient, reader: AIOKafkaConsumer) -> dict:
        cluster = await admin.describe_cluster()
        alive = {b["node_id"] for b in cluster["brokers"]}

        names = [t for t in await admin.list_topics() if not t.startswith(("lab.", "_"))]
        details = await admin.describe_topics(sorted(names))

        topics, under_replicated, offline, all_tps = [], 0, 0, []
        for item in details:
            partitions = []
            for p in sorted(item.get("partitions") or [], key=lambda p: p["partition"]):
                under_replicated += len(p["isr"]) < len(p["replicas"])
                offline += p["leader"] < 0
                partitions.append(
                    {"id": p["partition"], "leader": p["leader"], "replicas": p["replicas"], "isr": p["isr"]}
                )
                all_tps.append(TopicPartition(item["topic"], p["partition"]))
            topics.append({"name": item["topic"], "partitions": partitions})

        ends = await reader.end_offsets(all_tps) if all_tps else {}

        groups = []
        for group_id, _kind in await admin.list_consumer_groups():
            if group_id.startswith("lab."):
                continue
            offsets = await admin.list_consumer_group_offsets(group_id)
            lag = sum(max(0, ends.get(tp, m.offset) - m.offset) for tp, m in offsets.items())
            groups.append({"id": group_id, "lag": lag})

        return {
            "ok": True,
            "cluster_id": cluster["cluster_id"],
            "controller": cluster["controller_id"],
            "brokers": [{"id": b, "alive": b in alive} for b in EXPECTED_BROKERS],
            "under_replicated": under_replicated,
            "offline": offline,
            "topics": topics,
            "groups": sorted(groups, key=lambda g: g["id"]),
            "messages": {t["name"]: sum(ends.get(TopicPartition(t["name"], p["id"]), 0) for p in t["partitions"]) for t in topics},
            "at": time.time(),
        }


cluster_view = ClusterView()
