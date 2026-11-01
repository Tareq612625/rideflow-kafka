"""
RideFlow -- Feature Lab
=======================
One page where every Kafka feature can be run for real, against the real
cluster, with the real result shown beside an explanation.

    Page:    http://localhost:8003
    Docs:    http://localhost:8003/docs
    Verify:  docker compose exec lab-service python -m app.selftest
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from app import config, features  # noqa: F401  (importing registers every feature)
from app.cluster_view import cluster_view
from app.easy import BASICS, EASY
from app.lessons import GROUP_INTROS, LESSONS
from app.registry import FEATURES, GROUPS, run_feature


@asynccontextmanager
async def lifespan(app: FastAPI):
    cluster_view.start()
    yield
    await cluster_view.stop()


app = FastAPI(
    title="RideFlow Feature Lab",
    description="Run every Kafka feature against a real cluster.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
async def page():
    return FileResponse(config.STATIC_DIR / "lab.html")


@app.get("/health")
async def health():
    return {"service": config.SERVICE_NAME, "features": len(FEATURES), "kafka": config.BOOTSTRAP}


@app.get("/api/features")
async def list_features():
    return [
        {
            "name": group,
            "intro": GROUP_INTROS[group],
            "features": [
                {
                    "id": f.id, "title": f.title, "what": f.what, "why": f.why,
                    "code": f.code, "optional": f.optional,
                    "simple": EASY[f.id][0], "example": EASY[f.id][1],
                    "lesson": LESSONS[f.id],
                }
                for f in FEATURES.values() if f.group == group
            ],
        }
        for group in GROUPS
    ]


@app.get("/api/basics")
async def basics():
    return BASICS


@app.post("/api/features/{feature_id}/run")
async def run(feature_id: str, seconds: float | None = None):
    if feature_id not in FEATURES:
        raise HTTPException(404, f"no feature called {feature_id}")
    return await run_feature(feature_id, {"seconds": seconds} if seconds else None)


@app.get("/api/cluster")
async def cluster():
    return cluster_view.snapshot
