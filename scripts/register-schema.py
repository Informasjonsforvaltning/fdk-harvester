#!/usr/bin/env python3
"""Register HarvestEvent Avro schema with Confluent Schema Registry."""
import json
import os
import sys
import urllib.request

SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
SCHEMA_FILE = os.environ.get("SCHEMA_FILE", "/schemas/no.fdk.harvest.HarvestEvent.avsc")
SUBJECT = os.environ.get("SCHEMA_SUBJECT", "harvest-events-value")

def main():
    with open(SCHEMA_FILE) as f:
        schema_str = f.read()
    payload = json.dumps({"schema": schema_str}).encode("utf-8")
    url = f"{SCHEMA_REGISTRY_URL.rstrip('/')}/subjects/{SUBJECT}/versions"
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read().decode())
            print(f"Registered schema for subject '{SUBJECT}', version: {result.get('id', '?')}", file=sys.stderr)
    except urllib.error.HTTPError as e:
        if e.code == 409:
            print(f"Schema already registered for subject '{SUBJECT}'", file=sys.stderr)
            return
        raise

if __name__ == "__main__":
    main()
