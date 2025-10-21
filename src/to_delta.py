import argparse
import json
from datetime import datetime, timedelta, timezone

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
    parser.add_argument(
        "--rows-count",
        type=int,
        help="Number of rows to accumulate before writing to DeltaLake",
        default=10_000_000,
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
        "--delta-target-file-size",
        type=int,
        help="Delta target file size",
        default=1024 * 1024 * 256,
    )
    parser.add_argument(
        "--delta-write-batch-size",
        type=int,
        help="Delta write batch size",
        default=1000_000,
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
        write_batch_size=args.delta_write_batch_size,
        compression=args.delta_compression,
        compression_level=args.delta_compression_level,
    )

    col_map = {}
    if args.column_map_file:
        with open(args.column_map_file, "r") as f:
            col_map = json.load(f)

    if args.timestamp_column_name:
        if args.timestamp_start:
            timestamp_start = datetime.fromtimestamp(args.timestamp_start, timezone.utc)
        else:
            timestamp_start = datetime.now(timezone.utc)
        print(f"Timestamp start from {timestamp_start.isoformat()}")

    if col_map:
        convert_options = csv.ConvertOptions(
            include_columns=list(col_map.keys()),
            column_types={key: val["type"] for key, val in col_map.items()},
        )

    else:
        convert_options = None

    table = csv.read_csv(
        args.src_filepath,
        read_options=csv.ReadOptions(use_threads=True, block_size=args.read_block_size),
        convert_options=convert_options,
    )
    if col_map:
        table = table.rename_columns(
            {
                key: col_map[key]["name"]
                for key in col_map.keys()
                if "name" in col_map[key]
            }
        )

    repeat_idx = 0
    while repeat_idx < args.repeat_count:
        cnt = max(1, min(args.repeat_count, int(args.rows_count / table.num_rows)))
        dest_table = pa.concat_tables([table] * cnt).combine_chunks()
        repeat_idx += cnt

        if args.timestamp_column_name:
            timestamps = [
                timestamp_start + timedelta(seconds=i * args.timestamp_interval)
                for i in range(dest_table.num_rows)
            ]
            dest_table = dest_table.add_column(
                0, args.timestamp_column_name, pa.array(timestamps)
            )
            if args.date_column_name:
                dest_table = dest_table.add_column(
                    1,
                    args.date_column_name,
                    pa.array([ts.date() for ts in timestamps]),  # type: ignore
                )
            timestamp_start += timedelta(
                seconds=dest_table.num_rows * args.timestamp_interval
            )

        write_deltalake(
            table_or_uri=args.dest_path,
            data=dest_table,
            partition_by=args.delta_partition_by,
            mode="append",
            schema_mode="merge",
            configuration=delta_configurations,
            storage_options=delta_storage_options,
            target_file_size=args.delta_target_file_size,
            writer_properties=delta_writer_properties,
            post_commithook_properties=delta_post_commithook_properties,
        )
        print(
            f"[{repeat_idx}] Written to DeltaLake({args.dest_path}), number of rows: {dest_table.num_rows} at {datetime.now(timezone.utc).isoformat()}"
        )
