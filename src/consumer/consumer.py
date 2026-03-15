"""
Kafka → MinIO bronze consumer.
Reads raw-events topic and writes JSON files to:
  s3://datalake/bronze/events/raw-events/partition=<p>/part-<offset>.json
matching the path expected by the Spark transform.
"""
import json
import os
import time
import boto3
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "pipeline-kafka-kafka-bootstrap:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC",     "raw-events")
KAFKA_GROUP     = os.getenv("KAFKA_GROUP",     "bronze-consumer")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio.storage:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",     "datalake")
BRONZE_PREFIX    = os.getenv("BRONZE_PREFIX",    "bronze/events/raw-events")

FLUSH_SIZE_RECORDS = int(os.getenv("FLUSH_SIZE",       "50"))
FLUSH_INTERVAL_S   = int(os.getenv("FLUSH_INTERVAL_S", "30"))

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
    config=boto3.session.Config(signature_version="s3v4"),
)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id=KAFKA_GROUP,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda b: b.decode("utf-8"),
)

print(f"Consumer started. Topic={KAFKA_TOPIC}, flush every {FLUSH_SIZE_RECORDS} records or {FLUSH_INTERVAL_S}s")

buffer: dict[int, list] = {}  # partition → list of (offset, value)
last_flush = time.time()


def flush_partition(partition: int, records: list):
    if not records:
        return
    first_offset = records[0][0]
    lines = "\n".join(rec[1] for rec in records)
    key = f"{BRONZE_PREFIX}/partition={partition}/part-{first_offset:020d}.json"
    s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=lines.encode("utf-8"))
    print(f"  Wrote {len(records)} records → {key}")


def flush_all():
    for part, recs in buffer.items():
        flush_partition(part, recs)
    buffer.clear()


for msg in consumer:
    p = msg.partition
    buffer.setdefault(p, []).append((msg.offset, msg.value))

    total = sum(len(v) for v in buffer.values())
    now = time.time()

    if total >= FLUSH_SIZE_RECORDS or (now - last_flush) >= FLUSH_INTERVAL_S:
        flush_all()
        consumer.commit()
        last_flush = now
