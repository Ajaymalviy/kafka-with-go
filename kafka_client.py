# utils/kafka_client.py

from kafka import KafkaProducer, KafkaConsumer
import json

# ── single place to configure kafka ────────────────────────
KAFKA_BROKER = 'localhost:9092'

def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,

        # ── reliability configs ─────────────────────────────
        acks='all',
        # ↑ wait for ALL replicas to confirm
        # 'all' = strongest guarantee, no data loss
        # '1'   = only leader confirms, faster but risky
        # '0'   = fire and forget, fastest but no guarantee

        retries=3,
        # ↑ if send fails, retry 3 times automatically

        retry_backoff_ms=500
        # ↑ wait 500ms between retries
    )

def get_consumer(topic, group_id, offset_reset='earliest'):
    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER,
        group_id=group_id,
        auto_offset_reset=offset_reset,
        enable_auto_commit=False,          # always manual commit
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )
