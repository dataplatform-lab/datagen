import ast
import csv
import logging
import os
from collections.abc import Callable
from datetime import date
from io import StringIO
from typing import Literal, Protocol, Self
from urllib.parse import urlparse

import boto3
import bson
import orjson
import pyarrow as pa
import pyarrow.dataset as ds
from botocore.client import Config
from bson.codec_options import CodecOptions, TypeCodec, TypeRegistry
from fastnumbers import check_float

s3_config = Config(connect_timeout=30, read_timeout=30, retries={"max_attempts": 0})


def download_s3file(
    filepath: str,
    accesskey: str,
    secretkey: str,
    endpoint: str | None,
    region: str = "ap-northeast-2",
) -> str:
    logging.info("Downloading file %s from S3... %s", filepath, endpoint)
    tok = urlparse(filepath)

    src_bucket = tok.netloc
    src_key = tok.path.lstrip("/")
    _filepath = os.path.basename(tok.path)

    session = boto3.Session(
        aws_access_key_id=accesskey,
        aws_secret_access_key=secretkey,
        aws_session_token=None,
        region_name=region,
    )

    if endpoint and "amazonaws.com" in endpoint:
        endpoint = None

    s3 = session.client(service_name="s3", endpoint_url=endpoint, config=s3_config)
    meta_data = s3.head_object(Bucket=src_bucket, Key=src_key)
    total_length = int(meta_data.get("ContentLength", 0))

    PROGRESS = 0

    def _download_status(chunk):
        nonlocal PROGRESS
        PROGRESS += chunk
        done = int((PROGRESS / total_length) * 100000)
        if done % 1000 == 0:
            logging.info(
                "Processing... %s%% (%s/%s bytes)", done / 1000, PROGRESS, total_length
            )

    s3.download_file(src_bucket, src_key, _filepath, Callback=_download_status)
    logging.info("Download completed... %s", _filepath)

    return _filepath


class DateCodec(TypeCodec):
    @property
    def python_type(self):
        return date

    @property
    def bson_type(self):
        return str

    def transform_python(self, value: date):
        return str(value)

    def transform_bson(self, value: str):
        return date.fromisoformat(value)


codec_options = CodecOptions(type_registry=TypeRegistry([DateCodec()]))


def encode(value: dict | str, type: str) -> bytes:
    if type == "csv":
        return ",".join([str(val) for val in value.values()]).encode("utf-8")
    elif type == "json":
        return orjson.dumps(value, default=str)
    elif type == "bson" or type == "edge":
        return bson.encode(value, codec_options=codec_options)

    return value.encode("utf-8")


def decode(val: str | bytes, type: str):
    if type == "csv":
        f = StringIO(val.decode("utf-8"))
        for line in csv.reader(f):
            value = line
            break
    elif type == "json":
        value = orjson.loads(val.decode("utf-8"))
    elif type == "bson" or type == "edge":
        value = bson.decode(val)
    elif type == "txt":
        value = val.decode("utf-8")
    else:
        value = val

    return value


def csv_loads(value: str, headers: list[str]) -> dict:
    vals = []
    for v in value.strip().split(","):
        if v == "":
            vals.append(None)
        elif check_float(v):
            vals.append(float(v))
        else:
            vals.append(v)

    return {
        k: v for k, v in dict(zip(headers, vals, strict=False)).items() if v is not None
    }


def eval_create_func(eval_field_expr: str) -> Callable:
    fields = [
        node.id
        for node in ast.walk(ast.parse(eval_field_expr))
        if isinstance(node, ast.Name)
    ]
    return eval(
        "lambda " + ",".join(fields) + ",**kwargs" + ": " + eval_field_expr, {}, {}
    )


def load_rows(
    filepath: str, filetype: str = Literal["csv", "json", "jsonl", "bson"]
) -> list[dict]:
    rows = []
    if filetype == "bson":
        with open(filepath, "rb") as f:
            for line in bson.decode_file_iter(f):
                rows.append(line)
    else:
        with open(filepath, encoding="utf-8-sig") as f:
            if filetype == "jsonl":
                for line in f:
                    rows.append(orjson.loads(line))
            elif filetype == "json":
                obj = orjson.loads(f.read())
                if isinstance(obj, list):
                    rows.extend(obj)
                else:
                    rows.append(obj)

            elif filetype == "csv":
                headers = f.readline().strip().split(",")
                while line := f.readline():
                    if not line.strip():
                        break

                    row = csv_loads(line, headers)
                    if not row:
                        continue

                    rows.append(row)
            else:
                raise RuntimeError(f"Unsupported file type: {filetype}")

    return rows


class LoadAny(Protocol):
    def __init__(self, filepath: str): ...

    def __enter__(self) -> Self:
        return self

    def __exit__(self, type, value, trace_back): ...

    def rewind(self): ...

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> dict:
        return {}


class LoadBson:
    def __init__(self, filepath: str):
        self.io = open(filepath, "rb")

    def __enter__(self) -> Self:
        self.iter = bson.decode_file_iter(self.io)
        return self

    def __exit__(self, type, value, trace_back):
        self.io.close()

    def rewind(self):
        self.io.seek(0)
        self.iter = bson.decode_file_iter(self.io)

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> dict:
        return next(self.iter)


class LoadJson:
    def __init__(self, filepath: str):
        self.io = open(filepath, encoding="utf-8-sig")

    def __enter__(self) -> Self:
        self.json = orjson.loads(self.io.read())
        if isinstance(self.json, list):
            self.iter = iter(self.json)
        else:
            self.iter = iter([self.json])

        return self

    def __exit__(self, type, value, trace_back):
        self.io.close()

    def rewind(self):
        if isinstance(self.json, list):
            self.iter = iter(self.json)
        else:
            self.iter = iter([self.json])

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> dict:
        return next(self.iter)


class LoadJsonl:
    def __init__(self, filepath: str):
        self.io = open(filepath, encoding="utf-8-sig")

    def __enter__(self) -> Self:
        return self

    def __exit__(self, type, value, trace_back):
        self.io.close()

    def rewind(self):
        self.io.seek(0)

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> dict:
        line = self.io.readline()
        if not line:
            raise StopIteration

        return orjson.loads(line)


class LoadParquet:
    def __init__(
        self,
        filepath: str,
        batch_size: int = 1000,
        type: Literal["batch", "table", "python"] = "batch",
    ):
        self.filepath = filepath
        self.type = type
        self.batch_size = batch_size
        self.table = None

        self.batches = ds.dataset(self.filepath, format="parquet").to_batches(
            batch_size=self.batch_size
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, type, value, trace_back): ...

    def rewind(self):
        self.batches = ds.dataset(self.filepath, format="parquet").to_batches(
            batch_size=self.batch_size
        )

    def __iter__(self) -> Self:
        return self

    def __next__(self):
        batch = next(self.batches)

        if self.type == "python":
            return batch.to_pylist()
        elif self.type == "table":
            return pa.Table.from_batches([batch])

        return batch


class LoadCSV:
    def __init__(self, filepath: str):
        self.io = open(filepath, encoding="utf-8-sig")

    def __enter__(self) -> Self:
        self.headers = self.io.readline().strip().split(",")
        return self

    def __exit__(self, type, value, trace_back):
        self.io.close()

    def rewind(self):
        self.io.seek(0)
        self.headers = self.io.readline().strip().split(",")

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> dict:
        line = self.io.readline()
        if not line:
            raise StopIteration

        return csv_loads(line, self.headers)


def get_loader(filepath: str, input_type: str) -> LoadAny:
    if input_type == "bson":
        return LoadBson(filepath)
    if input_type == "json":
        return LoadJson(filepath)
    elif input_type == "jsonl":
        return LoadJsonl(filepath)
    # elif input_type == "parquet":
    #     return LoadParquet(filepath)
    elif input_type == "csv":
        return LoadCSV(filepath)
    else:
        raise RuntimeError(f"Unsupported input type: {input_type}")
