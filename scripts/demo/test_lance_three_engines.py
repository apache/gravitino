#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Smoke test: verify that lance-ray, PyLance and Daft can read/write the same
# Lance table managed by Gravitino's Lance REST namespace, with data persisted
# to MinIO (S3 compatible).
#
# Prerequisites (already up on this machine per ENVIRONMENT.md):
#   - MinIO at http://127.0.0.1:9000 (bucket `contacts`)
#   - Gravitino at http://127.0.0.1:8090
#   - Lance REST at http://127.0.0.1:9101/lance
#   - Metalake `test`, catalog `lance_catalog` (location s3://contacts/raw/lance)
#
# Python env:
#   source ~/venvs/lancedb-env/bin/activate
#   pip install "lance-namespace<=0.4.5" lance-ray pylance daft ray pyarrow

import os
import sys

import pyarrow as pa

# ---------- configuration ----------

LANCE_REST_URI = "http://127.0.0.1:9101/lance"
CATALOG = "lance_catalog"
SCHEMA = "sales"
TABLE = "orders"
TABLE_ID = [CATALOG, SCHEMA, TABLE]

MINIO_ENDPOINT = "http://127.0.0.1:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_REGION = "us-east-1"

# Passed to lance/lance-ray write_lance/read_lance and lance.dataset(...).
STORAGE_OPTIONS = {
    "access_key_id": MINIO_ACCESS_KEY,
    "secret_access_key": MINIO_SECRET_KEY,
    "endpoint": MINIO_ENDPOINT,
    "region": MINIO_REGION,
    "allow_http": "true",
    "virtual_hosted_style_request": "false",
}

# Mirror credentials into AWS_* env vars as a fallback for any underlying
# object_store layer that ignores explicit storage_options.
os.environ.setdefault("AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
os.environ.setdefault("AWS_ENDPOINT_URL", MINIO_ENDPOINT)
os.environ.setdefault("AWS_REGION", MINIO_REGION)
os.environ.setdefault("AWS_DEFAULT_REGION", MINIO_REGION)
os.environ.setdefault("AWS_ALLOW_HTTP", "true")


def log(stage: str, msg: str) -> None:
    print(f"[{stage}] {msg}", flush=True)


# ---------- step 1: connect to Lance REST namespace ----------

def connect_namespace():
    import lance_namespace as ln

    log("ns", f"connecting to Lance REST at {LANCE_REST_URI}")
    return ln.connect("rest", {"uri": LANCE_REST_URI})


# ---------- step 2: ensure schema exists ----------

def ensure_schema(namespace) -> None:
    """Create namespace [lance_catalog, sales] if it does not already exist."""
    from lance_namespace import CreateNamespaceRequest, NamespaceExistsRequest

    try:
        namespace.namespace_exists(NamespaceExistsRequest(id=[CATALOG, SCHEMA]))
        log("ns", f"schema {CATALOG}.{SCHEMA} already exists")
        return
    except Exception:
        pass

    namespace.create_namespace(
        CreateNamespaceRequest(id=[CATALOG, SCHEMA], mode="CREATE")
    )
    log("ns", f"created schema {CATALOG}.{SCHEMA}")


# ---------- step 3: write with lance-ray ----------

NAMESPACE_PROPERTIES = {"uri": LANCE_REST_URI}
NAMESPACE_IMPL = "rest"


def write_with_lance_ray() -> None:
    import ray
    from lance_ray import write_lance

    ray.init(ignore_reinit_error=True, logging_level="WARNING")

    table = pa.table(
        {
            "id": pa.array(range(1000), type=pa.int64()),
            "value": pa.array([i * 2 for i in range(1000)], type=pa.int64()),
        }
    )
    data = ray.data.from_arrow(table)

    log("lance-ray", f"writing 1000 rows to {'.'.join(TABLE_ID)}")
    write_lance(
        data,
        table_id=TABLE_ID,
        namespace_impl=NAMESPACE_IMPL,
        namespace_properties=NAMESPACE_PROPERTIES,
        mode="overwrite",
        storage_options=STORAGE_OPTIONS,
    )
    log("lance-ray", "write OK")


# ---------- step 4: read with lance-ray ----------

def read_with_lance_ray() -> int:
    from lance_ray import read_lance

    ds = read_lance(
        table_id=TABLE_ID,
        namespace_impl=NAMESPACE_IMPL,
        namespace_properties=NAMESPACE_PROPERTIES,
        storage_options=STORAGE_OPTIONS,
    )
    rows = ds.count()
    log("lance-ray", f"read back rows = {rows}")
    return rows


# ---------- step 5: resolve table URI via Lance REST describe ----------

def describe_table_uri(namespace) -> str:
    from lance_namespace import DescribeTableRequest

    resp = namespace.describe_table(DescribeTableRequest(id=TABLE_ID))
    location = getattr(resp, "location", None)
    if not location and hasattr(resp, "to_dict"):
        location = resp.to_dict().get("location")
    if not location:
        raise RuntimeError(f"describe_table returned no location: {resp}")
    log("describe", f"table_uri = {location}")
    return location


# ---------- step 6: read with PyLance ----------

def read_with_pylance(table_uri: str) -> int:
    import lance

    ds = lance.dataset(table_uri, storage_options=STORAGE_OPTIONS)
    rows = ds.count_rows()
    head = ds.to_table(limit=5).to_pydict()
    log("pylance", f"rows = {rows}, head = {head}")
    return rows


# ---------- step 7: read with Daft ----------

def read_with_daft(table_uri: str) -> int:
    import daft
    from daft.io import IOConfig, S3Config

    io = IOConfig(
        s3=S3Config(
            endpoint_url=MINIO_ENDPOINT,
            key_id=MINIO_ACCESS_KEY,
            access_key=MINIO_SECRET_KEY,
            region_name=MINIO_REGION,
            use_ssl=False,
            force_virtual_addressing=False,
        )
    )
    df = daft.read_lance(table_uri, io_config=io)
    rows = df.count_rows()
    log("daft", f"rows = {rows}")
    df.show(5)
    return rows


# ---------- main ----------

def main() -> int:
    ns = connect_namespace()
    ensure_schema(ns)

    write_with_lance_ray()
    ray_rows = read_with_lance_ray()

    table_uri = describe_table_uri(ns)
    pylance_rows = read_with_pylance(table_uri)
    daft_rows = read_with_daft(table_uri)

    expected = 1000
    failures = []
    if ray_rows != expected:
        failures.append(f"lance-ray rows={ray_rows} expected={expected}")
    if pylance_rows != expected:
        failures.append(f"pylance rows={pylance_rows} expected={expected}")
    if daft_rows != expected:
        failures.append(f"daft rows={daft_rows} expected={expected}")

    if failures:
        log("FAIL", "; ".join(failures))
        return 1

    log("PASS", "lance-ray + pylance + daft all read 1000 rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
