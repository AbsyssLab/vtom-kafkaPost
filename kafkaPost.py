#!/usr/bin/env python3
# """
# Kafka Post script for VTOM: publish a message.

# Positional parameters:
  # $1  bootstrap_servers
  # $2  topic
  # $3  message

# Environment variables:
  # KAFKA_KEY                 optional Kafka message key
  # KAFKA_SECURITY_PROTOCOL   default: PLAINTEXT
  # KAFKA_SASL_MECHANISM
  # KAFKA_USERNAME
  # KAFKA_PASSWORD
  # KAFKA_SSL_CAFILE

# Exit codes:
  # 0 = message published
  # 1 = error
# """

import logging
import os
import sys

from kafka import KafkaProducer
from kafka.serializer import DefaultSerializer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
    force=True,
)

# === Configuration from environment ===
KAFKA_KEY = os.getenv("KAFKA_KEY", "")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")
KAFKA_SSL_CAFILE = os.getenv("KAFKA_SSL_CAFILE", "")

# === Argument validation ===
if len(sys.argv) < 4:
    logging.error(
        "Usage: python kafkaPost.py <bootstrap_servers> <topic> <message>"
    )
    logging.error(
        "Environment variables: KAFKA_KEY, KAFKA_SECURITY_PROTOCOL, "
        "KAFKA_SASL_MECHANISM, KAFKA_USERNAME, KAFKA_PASSWORD, KAFKA_SSL_CAFILE"
    )
    sys.exit(1)

BOOTSTRAP_SERVERS = sys.argv[1]
TOPIC = sys.argv[2]
MESSAGE = sys.argv[3]


def build_producer():
    """Build a configured KafkaProducer."""
    config = {
        "bootstrap_servers": [
            item.strip() for item in BOOTSTRAP_SERVERS.split(",") if item.strip()
        ],
        "security_protocol": KAFKA_SECURITY_PROTOCOL or "PLAINTEXT",
        "acks": "all",
        "retries": 3,
        "key_serializer": DefaultSerializer(),
        "value_serializer": DefaultSerializer(),
    }

    if KAFKA_SASL_MECHANISM:
        config["sasl_mechanism"] = KAFKA_SASL_MECHANISM
    if KAFKA_USERNAME:
        config["sasl_plain_username"] = KAFKA_USERNAME
    if KAFKA_PASSWORD:
        config["sasl_plain_password"] = KAFKA_PASSWORD
    if KAFKA_SSL_CAFILE:
        config["ssl_cafile"] = KAFKA_SSL_CAFILE

    return KafkaProducer(**config)


# === Main execution ===
producer = None
try:
    logging.info("Starting Kafka Post publish")
    logging.info(f"Servers           : {BOOTSTRAP_SERVERS}")
    logging.info(f"Topic             : {TOPIC}")
    logging.info(
        f"Key               : {'(none)' if KAFKA_KEY == '' else '(provided)'}"
    )
    logging.info("Message           : (not displayed)")
    logging.info(f"Security protocol : {KAFKA_SECURITY_PROTOCOL}")

    logging.info("Connecting to Kafka broker...")
    producer = build_producer()
    logging.info("Successfully connected to Kafka")

    logging.info("Publishing message...")
    future = producer.send(
        TOPIC,
        value=MESSAGE,
        key=KAFKA_KEY if KAFKA_KEY != "" else None,
    )
    metadata = future.get(timeout=10)
    producer.flush()

    logging.info(
        f"Message published (partition={metadata.partition}, "
        f"offset={metadata.offset})"
    )
    logging.info("Result: OK (code 0)")
    sys.exit(0)

except Exception as exc:
    logging.error(f"Error during execution: {exc}")
    sys.exit(1)

finally:
    if producer is not None:
        logging.info("Closing Kafka producer")
        producer.close()
