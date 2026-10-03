#!/usr/bin/env python3
"""
Ingests the synthetic CME Gemini Enterprise logs into Google Cloud Logging
via the Cloud Logging API (`entries.write`) in batches of 250:

1. Type 1 (400 Logs): `data/raw_ge_chat_connector_logs/ge_chat_connector_logs_400.jsonl`
   -> Written to log stream `gemini-enterprise-chat-connector-telemetry`
2. Type 2 (600 Logs): `data/raw_agent_tree_logs/agent_tree_logs_600.jsonl`
   -> Written to log stream `gemini-enterprise-agent-telemetry`

Usage:
  python3 scripts/ingest_to_cloud_logging.py --project YOUR_GCP_PROJECT_ID [--source all|chat|agent]
"""

import argparse
import datetime
import json
import os
import pathlib
import sys
import time
from google.cloud import logging as cloud_logging

BASE_DIR = pathlib.Path(__file__).resolve().parent.parent
CHAT_JSONL = BASE_DIR / "data" / "raw_ge_chat_connector_logs" / "ge_chat_connector_logs_400.jsonl"
AGENT_JSONL = BASE_DIR / "data" / "raw_agent_tree_logs" / "agent_tree_logs_600.jsonl"

CHAT_LOG_NAME = "gemini-enterprise-chat-connector-telemetry"
AGENT_LOG_NAME = "gemini-enterprise-agent-telemetry"


def ingest_file(project_id: str, jsonl_path: pathlib.Path, log_name: str, batch_size: int = 250) -> None:
    """Reads a JSONL log file and writes its entries to Cloud Logging in batches."""
    client = cloud_logging.Client(project=project_id)
    logger = client.logger(log_name)

    with open(jsonl_path, "r", encoding="utf-8") as f:
        records = [json.loads(line) for line in f]

    total = len(records)
    print(f"\nLoaded {total} records from {jsonl_path.relative_to(BASE_DIR)}")
    print(f"Writing to Cloud Logging stream: projects/{project_id}/logs/{log_name} ...")

    for start in range(0, total, batch_size):
        chunk = records[start : start + batch_size]
        batch = logger.batch()
        for rec in chunk:
            ts_str = rec["timestamp"]
            ts_dt = datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            res = cloud_logging.Resource(
                type="global",
                labels={"project_id": project_id},
            )
            payload = rec["jsonPayload"]
            entry_labels = {
                "cme_demo": "gemini-enterprise-prompt-analytics",
                "interaction_type": payload.get("interaction_type", "UNKNOWN"),
                "simulated_service": (
                    "discoveryengine.googleapis.com"
                    if payload.get("interaction_type") == "GE_CHAT_CONNECTOR_SEARCH"
                    else "aiplatform.googleapis.com/ReasoningEngine"
                ),
                "pillar": payload.get("pillar", "Unknown"),
                "plant_location": payload.get("plant_location", "Unknown"),
            }
            batch.log_struct(
                payload,
                severity=rec.get("severity", "INFO"),
                timestamp=ts_dt,
                insert_id=rec.get("insertId"),
                trace=f"projects/{project_id}/traces/{rec['trace'].split('/')[-1]}",
                span_id=rec.get("spanId"),
                resource=res,
                labels=entry_labels,
            )
        batch.commit()
        print(f"  Committed batch {start + 1}..{start + len(chunk)} / {total}")
        time.sleep(0.2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch-ingest CME synthetic logs into Cloud Logging")
    parser.add_argument(
        "--project",
        default=os.environ.get("GOOGLE_CLOUD_PROJECT", ""),
        help="GCP Project ID (or set GOOGLE_CLOUD_PROJECT)",
    )
    parser.add_argument(
        "--source",
        choices=["all", "chat", "agent"],
        default="all",
        help="Which log source to ingest: 'all' (1,000), 'chat' (400 GE Chat Connector logs), or 'agent' (600 Agent Tree logs)",
    )
    args = parser.parse_args()

    if not args.project:
        print("Error: Please specify --project YOUR_PROJECT_ID or set GOOGLE_CLOUD_PROJECT.", file=sys.stderr)
        sys.exit(1)

    if args.source in ("all", "chat"):
        ingest_file(args.project, CHAT_JSONL, CHAT_LOG_NAME)
    if args.source in ("all", "agent"):
        ingest_file(args.project, AGENT_JSONL, AGENT_LOG_NAME)

    print("\n[SUCCESS] Ingestion complete!")


if __name__ == "__main__":
    main()
