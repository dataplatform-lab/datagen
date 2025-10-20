import argparse
from utils import LoadParquet

import pyarrow as pa
from deltalake import PostCommitHookProperties, WriterProperties, write_deltalake


def _write_to_delta_table(rows: list[dict]):
    schema = pa.schema(
        [
            ("timestamp", pa.string()),
            ("date", pa.string()),
            ("string0", pa.string()),
            ("int0", pa.int32()),
            ("float0", pa.float64()),
            ("timestamp0", pa.string()),
            ("date0", pa.string()),
        ]
    )

    tb = pa.Table.from_pylist(rows, schema=schema)

    tb = (
        tb.set_column(
            tb.schema.get_field_index("timestamp"),
            "timestamp",
            tb.column("timestamp").cast(pa.timestamp("ns", tz="UTC")),
        )
        .set_column(
            tb.schema.get_field_index("timestamp0"),
            "timestamp0",
            tb.column("timestamp0").cast(pa.timestamp("ns", tz="UTC")),
        )
        .set_column(
            tb.schema.get_field_index("date"),
            "date",
            tb.column("date").cast(pa.date32()),
        )
        .set_column(
            tb.schema.get_field_index("date0"),
            "date0",
            tb.column("date0").cast(pa.date32()),
        )
    )

    write_deltalake(
        table_or_uri=args.dest_filepath,
        data=tb,
        partition_by="date",
        mode="append",
        schema_mode="merge",
        configuration=delta_configurations,
        storage_options=delta_storage_options,
        target_file_size=1024 * 1024 * 128,
        writer_properties=delta_writer_properties,
        post_commithook_properties=delta_post_commithook_properties,
    )
    print(
        f"Written to DeltaLake({args.dest_filepath}), number of rows: {tb.num_rows}... Done"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--src-filepath", required=True, type=str, help="Source filepath"
    )
    parser.add_argument(
        "--src-filetype", required=True, choices=["parquet"], help="Source filetype"
    )
    parser.add_argument(
        "--dest-path", required=True, type=str, help="Destination delta table path"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100_000,
        help="Batch size for writing to Delta",
    )

    # Delta parameters
    parser.add_argument(
        "--delta-partition-by",
        type=str,
        action="append",
        help="Delta partition by",
        default=None,
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
        default=1_048_576,
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
            "AWS_ALLOW_HTTP": "true"
            if args.s3_endpoint.startswith("http")
            else "false",
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
        data_page_size_limit=args.delta_page_size_limit,
        compression=args.delta_compression,
        compression_level=args.delta_compression_level,
    )

    if args.src_filetype == "parquet":
        with LoadParquet(
            args.src_filepath, batch_size=args.batch_size, type="batch"
        ) as loader:
            for table in loader:
                write_deltalake(
                    table_or_uri=args.dest_path,
                    data=table,
                    partition_by=args.delta_partition_by,
                    mode="append",
                    schema_mode="merge",
                    configuration=delta_configurations,
                    storage_options=delta_storage_options,
                    target_file_size=1024 * 1024 * 128,
                    writer_properties=delta_writer_properties,
                    post_commithook_properties=delta_post_commithook_properties,
                )
                print(
                    f"Written to DeltaLake({args.dest_path}), number of rows: {table.num_rows}... Done"
                )
