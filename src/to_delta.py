import argparse
import json
from datetime import UTC, datetime, timedelta

import pyarrow as pa
from deltalake import PostCommitHookProperties, WriterProperties, write_deltalake
from pyarrow import csv

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--src-filepath", type=str, help="Source filepath", required=True
    )
    parser.add_argument(
        "--dest-path", type=str, help="Destination table path", required=True
    )

    parser.add_argument(
        "--repeat-count",
        type=int,
        help="Number of times to repeat the write process",
        default=1,
    )
    parser.add_argument(
        "--read-block-size",
        type=int,
        help="Bytes to read at a time from source file",
        default=1024 * 1024 * 1024,
    )

    # Timestamp options
    parser.add_argument(
        "--timestamp-column-name",
        type=str,
        help="timestamp column name",
    )
    parser.add_argument(
        "--timestamp-start",
        type=float,
        help="timestamp start in epoch seconds",
    )
    parser.add_argument(
        "--timestamp-interval",
        type=float,
        help="timestamp difference in seconds",
        default=1.0,
    )
    # Date options
    parser.add_argument(
        "--date-column-name",
        type=str,
        help="date column name",
    )

    # Column options
    parser.add_argument(
        "--column-map-file",
        type=str,
        help="Path to the column mapping file (JSON format)",
    )

    # Delta parameters
    parser.add_argument(
        "--delta-partition-by",
        type=str,
        action="append",
        help="Delta partitioned by",
    )
    parser.add_argument(
        "--delta-append-only",
        action=argparse.BooleanOptionalAction,
        help="Delta append only",
        default=False,
    )
    parser.add_argument(
        "--delta-log-retention-duration",
        type=str,
        default="interval 7 day",
        help="Delta log retention duration",
    )
    parser.add_argument(
        "--delta-deleted-file-retention-duration",
        type=str,
        default="interval 1 day",
        help="Delta deleted file retention duration",
    )
    parser.add_argument(
        "--delta-page-size-limit",
        type=int,
        help="Delta page size limit",
    )
    parser.add_argument(
        "--delta-compression", type=str, default="ZSTD", help="Delta compression"
    )
    parser.add_argument(
        "--delta-compression-level",
        type=int,
        default=3,
        help="Delta compression level",
    )

    # S3 parameters
    parser.add_argument(
        "--s3-endpoint",
        help="S3 endpoint",
        default="https://s3.amazonaws.com",
    )
    parser.add_argument("--s3-accesskey", help="S3 accesskey")
    parser.add_argument("--s3-secretkey", help="S3 secretkey")
    parser.add_argument("--s3-region", help="S3 region", default="ap-northeast-2")
    parser.add_argument(
        "--s3-allow-unsafe-rename",
        action=argparse.BooleanOptionalAction,
        help="S3 allow unsafe rename",
        default=True,
    )
    args = parser.parse_args()

    delta_configurations = {
        "delta.checkpointPolicy": "v2",
        "delta.appendOnly": "false" if not args.delta_append_only else "true",
        "delta.logRetentionDuration": args.delta_log_retention_duration,
        "delta.deletedFileRetentionDuration": args.delta_deleted_file_retention_duration,
        "delta.dataSkippingNumIndexedCols": "-1",
    }
    delta_storage_options = None
    if args.dest_path.startswith("s3://"):
        delta_storage_options = {
            "AWS_ACCESS_KEY_ID": args.s3_accesskey,
            "AWS_SECRET_ACCESS_KEY": args.s3_secretkey,
            "AWS_REGION": "ap-northeast-2",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_ALLOW_HTTP": (
                "true" if args.s3_endpoint.startswith("http") else "false"
            ),
        }
        if "s3.amazonaws.com" not in args.s3_endpoint:
            delta_storage_options = {
                "AWS_ENDPOINT_URL": args.s3_endpoint,
                **delta_storage_options,
            }
    delta_post_commithook_properties = PostCommitHookProperties(
        create_checkpoint=True,
        cleanup_expired_logs=True,
    )
    delta_writer_properties = WriterProperties(
        write_batch_size=args.repeat_count,
        compression=args.delta_compression,
        compression_level=args.delta_compression_level,
    )

    col_map = {}
    if args.column_map_file:
        with open(args.column_map_file, "r") as f:
            col_map = json.load(f)

    if args.timestamp_column_name:
        if args.timestamp_start:
            timestamp_start = datetime.fromtimestamp(args.timestamp_start, UTC)
        else:
            timestamp_start = datetime.now(UTC)
        print(f"Timestamp start from {timestamp_start.isoformat()}")

    for repeat_idx in range(args.repeat_count):
        if col_map:
            convert_options = csv.ConvertOptions(
                include_columns=list(col_map.keys()),
                column_types={key: val["type"] for key, val in col_map.items()},
            )

        else:
            convert_options = None

        batches = []
        reader = csv.open_csv(
            args.src_filepath,
            read_options=csv.ReadOptions(
                use_threads=True,
                block_size=args.read_block_size if args.read_block_size else None,
            ),
            convert_options=convert_options,
        )
        for batch in reader:
            if col_map:
                batch = batch.rename_columns(
                    {
                        key: col_map[key]["name"]
                        for key in col_map.keys()
                        if "name" in col_map[key]
                    }
                )

            if args.timestamp_column_name:
                batch = batch.add_column(
                    0,
                    args.timestamp_column_name,
                    [
                        timestamp_start + timedelta(seconds=i * args.timestamp_interval)
                        for i in range(batch.num_rows)
                    ],
                )
                if args.date_column_name:
                    batch = batch.add_column(
                        1,
                        args.date_column_name,
                        [
                            (
                                timestamp_start
                                + timedelta(seconds=i * args.timestamp_interval)
                            ).date()  # type: ignore
                            for i in range(batch.num_rows)
                        ],
                    )

            batches.append(batch)

        table = pa.Table.from_batches(batches)
        write_deltalake(
            table_or_uri=args.dest_path,
            data=table,
            partition_by=args.delta_partition_by,
            mode="append",
            schema_mode="merge",
            configuration=delta_configurations,
            storage_options=delta_storage_options,
            target_file_size=1024 * 1024 * 256,
            writer_properties=delta_writer_properties,
            post_commithook_properties=delta_post_commithook_properties,
        )
        print(
            f"[{repeat_idx}] Written to DeltaLake({args.dest_path}), number of rows: {table.num_rows} at {datetime.now(UTC).isoformat()}"
        )
