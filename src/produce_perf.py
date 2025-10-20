#!python

# https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#producer
# https://github.com/confluentinc/confluent-kafka-python/tree/master/examples

import argparse
import logging
import time
from datetime import UTC, datetime, timedelta
from time import perf_counter

import pandas as pd
from confluent_kafka import KafkaException, Producer

from utils.utils import download_s3file, encode, load_rows

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Kafka
    parser.add_argument(
        "--kafka-bootstrap-servers",
        help="Kafka bootstrap servers",
        default="redpanda.redpanda.svc.cluster.local:9093",
    )
    parser.add_argument(
        "--kafka-security-protocol",
        help="Kafka security protocol",
        default="SASL_PLAINTEXT",
    )
    parser.add_argument(
        "--kafka-sasl-mechanism", help="Kafka SASL mechanism", default="SCRAM-SHA-512"
    )
    parser.add_argument(
        "--kafka-sasl-username", help="Kafka SASL plain username", required=True
    )
    parser.add_argument(
        "--kafka-sasl-password", help="Kafka SASL plain password", required=True
    )
    parser.add_argument(
        "--kafka-ssl-ca-location", help="Kafka SSL CA file", default=None
    )
    parser.add_argument(
        "--kafka-auto-offset-reset",
        help="Kafka auto offset reset (earliest/latest)",
        default="latest",
    )
    parser.add_argument("--kafka-topic", help="Kafka topic name", required=True)
    parser.add_argument(
        "--kafka-partition", help="Kafka partition", type=int, default=0
    )
    parser.add_argument("--kafka-key", help="Kafka partition key", default=None)
    parser.add_argument(
        "--kafka-compression-type",
        help="Kafka producer compression type",
        choices=["gzip", "snappy", "lz4", "zstd", "none"],
        default="gzip",
    )
    parser.add_argument(
        "--kafka-delivery-timeout-ms",
        help="Kafka delivery timeout in ms",
        default=30000,
    )
    parser.add_argument("--kafka-linger-ms", help="Kafka linger ms", default=1000)
    parser.add_argument(
        "--kafka-batch-size",
        help="Kafka maximum size of size (in bytes) of all messages batched in one MessageSet",
        default=1000000,
    )
    parser.add_argument(
        "--kafka-batch-num-messages",
        help="Kafka maximum number of messages batched in one MessageSet",
        default=10000,
    )
    parser.add_argument(
        "--kafka-message-max-bytes", help="Kafka message max bytes", default=1000000
    )
    parser.add_argument(
        "--kafka-acks",
        help="Kafka idempotent delivery option ",
        choices=[1, 0, -1],
        type=int,
        default=0,
    )

    # Output
    parser.add_argument(
        "--output-type",
        help="Output message type",
        choices=["csv", "json", "bson"],
        default="json",
    )
    parser.add_argument(
        "--custom-row",
        help="Custom key values (e.g. edge=test-edge)",
        nargs="*",
        default=[],
    )

    # Rate
    parser.add_argument(
        "--rate",
        help="Number of records to be produced for each rate interval",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--rate-interval",
        help="Rate interval in seconds",
        type=float,
        default=None,
    )

    # Timestamp options
    parser.add_argument(
        "--timestamp-start",
        help="timestamp start in epoch seconds",
        type=float,
        default=None,  # now
    )

    # Record interval
    parser.add_argument(
        "--interval",
        help="timestamp difference in seconds",
        type=float,
        default=None,  # args.rate_interval/args.rate
    )

    # File
    parser.add_argument("--input-filepath", help="file to be produced", required=True)
    parser.add_argument(
        "--input-type",
        help="Input file type",
        choices=["csv", "jsonl", "bsonl"],
        default="jsonl",
    )
    parser.add_argument(
        "--s3-endpoint",
        help="S3 url",
        default="http://rook-ceph-rgw-ceph-objectstore.rook-ceph.svc.cluster.local:80",
    )
    parser.add_argument("--s3-accesskey", help="S3 accesskey")
    parser.add_argument("--s3-secretkey", help="S3 secretkey")

    parser.add_argument("--loop-max", help="maximum loop count", type=int, default=1)

    parser.add_argument(
        "--result-filepath",
        help="result csv file path to be saved (e.g. s3a://ingkle-test/results/filename.csv)",
        default=None,
    )

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    custom_row = {}
    for kv in args.custom_row:
        key, val = kv.split("=")
        custom_row[key] = val

    # https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html
    # https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    configs = {
        "auto.offset.reset": args.kafka_auto_offset_reset,
        "bootstrap.servers": args.kafka_bootstrap_servers,
        "security.protocol": args.kafka_security_protocol,
        "compression.type": args.kafka_compression_type,
        "delivery.timeout.ms": args.kafka_delivery_timeout_ms,
        "linger.ms": args.kafka_linger_ms,
        "batch.size": args.kafka_batch_size,
        "batch.num.messages": args.kafka_batch_num_messages,
        "message.max.bytes": args.kafka_message_max_bytes,
        "queue.buffering.max.messages": 2147483647,
        "queue.buffering.max.kbytes": 2147483647,
        "queue.buffering.max.ms": 900000,
        "acks": args.kafka_acks,
    }
    if args.kafka_security_protocol.startswith("SASL"):
        configs.update(
            {
                "sasl.mechanism": args.kafka_sasl_mechanism,
                "sasl.username": args.kafka_sasl_username,
                "sasl.password": args.kafka_sasl_password,
            }
        )
    if args.kafka_security_protocol.endswith("SSL"):
        # * Mac: brew install openssl
        # * Ubuntu: sudo apt install ca-certificates
        configs.update(
            {
                # ! https://github.com/confluentinc/confluent-kafka-python/issues/1610
                "enable.ssl.certificate.verification": False,
                "ssl.ca.location": args.kafka_ssl_ca_location,
            }
        )

    producer = Producer(configs)
    configs.pop("sasl.password", None)
    logging.info("Producer created:")
    logging.info(configs)

    filepath = args.input_filepath
    if filepath.startswith("s3a://"):
        filepath = download_s3file(
            filepath, args.s3_accesskey, args.s3_secretkey, args.s3_endpoint
        )

    values = load_rows(filepath, args.input_type)
    if not values and not custom_row:
        logging.warning("No values to be produced")
        exit(0)

    row_idx = 0
    timestamp_start = datetime.fromtimestamp(args.timestamp_start, UTC)
    total_elapsed = []
    for loop_idx in range(args.loop_max):
        now = datetime.now(UTC)

        rows = []
        for _ in range(args.rate):
            at = datetime.now(UTC)
            rows.append(
                values[row_idx]
                | custom_row
                | {
                    "timestamp": int(timestamp_start.timestamp() * 1e6),
                    "created_at": at,
                    "updated_at": at,
                }
            )
            row_idx = (row_idx + 1) % len(values)
            timestamp_start += timedelta(microseconds=args.interval * 1e6)

        start = perf_counter()
        producer.poll(0)
        for row in rows:
            try:
                producer.produce(
                    topic=args.kafka_topic,
                    value=encode(row, args.output_type),
                    key=args.kafka_key.encode("utf-8") if args.kafka_key else None,
                    partition=args.kafka_partition,
                )
            except KafkaException as e:
                logging.error("KafkaException: %s", e)

        producer.flush()
        elapsed = perf_counter() - start
        total_elapsed.append(elapsed)
        logging.info(f"{loop_idx}: elapsed: {elapsed}s, start: {now}")

        if args.rate_interval:
            wait = args.rate_interval - (datetime.now(UTC) - now).total_seconds()
            wait = 0.0 if wait < 0 else wait

            logging.info("Waiting for %f seconds...", wait)
            time.sleep(wait)
    producer.flush()
    logging.info("Finished")

    df = pd.DataFrame({"elapsed": total_elapsed})
    print(df)
    print(df.mean())

    if args.result_filepath:
        df.to_csv(
            args.result_filepath,
            index=False,
            storage_options=(
                {
                    "key": args.s3_accesskey,
                    "secret": args.s3_secretkey,
                    "endpoint_url": args.s3_endpoint,
                }
                if args.result_filepath.startswith("s3a://")
                else None
            ),
        )
        logging.info("Results saved to %s", args.result_filepath)
