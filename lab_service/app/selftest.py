"""Run every feature and print the result. It exits with 1 if anything failed.

    docker compose exec lab-service python -m app.selftest
    docker compose exec lab-service python -m app.selftest --only keys,acks
"""

import argparse
import asyncio
import json
import sys
from app import features  # noqa: F401  (importing registers every feature)
from app.registry import FEATURES, GROUPS, run_feature


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", help="comma-separated feature ids")
    parser.add_argument("--json", action="store_true", help="print one JSON result per line")
    arguments = parser.parse_args()

    in_page_order = sorted(FEATURES.values(), key=lambda f: GROUPS.index(f.group))
    ids = arguments.only.split(",") if arguments.only else [f.id for f in in_page_order]
    counts = {"passed": 0, "failed": 0, "skipped": 0}

    for feature_id in ids:
        result = await run_feature(feature_id)
        counts[result["status"]] += 1
        if arguments.json:
            print(json.dumps(result), flush=True)
            continue
        print(f'{result["status"].upper():8} {feature_id:22} {result["seconds"]:6.1f}s', flush=True)
        for item in result["items"]:
            if item["kind"] == "check" and not item["ok"]:
                print(f'           failed check: {item["label"]} ({item["detail"]})', flush=True)
        if result["error"]:
            print(f'           {result["error"]}', flush=True)

    print(f'\n{counts["passed"]} passed, {counts["failed"]} failed, {counts["skipped"]} skipped', flush=True)
    return 1 if counts["failed"] else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
