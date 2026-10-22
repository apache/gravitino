#!/usr/bin/env python3
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

"""Measure JDBC connection churn and HTTP tail latency under concurrent reads.

Run the same workload against fresh server processes with only the idle limit changed, for
example::

    python3 dev/performance/jdbc_idle_retention.py \\
      --url http://localhost:8090/api/metalakes \\
      --mysql-defaults-extra-file=/path/to/mysql-client.cnf \\
      --clients 64 --warmup-seconds 3 --seconds 15 --rounds 3

The MySQL client option file supplies the database credentials. Set
GRAVITINO_BENCH_AUTHORIZATION to send an Authorization header. Use a catalog read URL, such as
the list-tables route, to test a JDBC catalog pool instead of the entity-store pool.

Each round prints successful throughput, p50/p99 latency, failures, and the change in MySQL's
server-wide Connections and Threads_created counters. Those counters include this script's own
status queries and any other database traffic, so compare runs on an otherwise quiet database with
the same client count, data, JVM settings and node count, and only after confirming that no
requests failed.

Self-test::

    cd dev/performance && PYTHONDONTWRITEBYTECODE=1 python3 -m unittest test_jdbc_idle_retention
"""

import argparse
import concurrent.futures
import http.client
import json
import math
import os
import subprocess
import sys
import time
from urllib.parse import urlsplit


def percentile(samples, fraction):
    """Return a nearest-rank percentile in milliseconds."""
    if not samples:
        return None
    ordered = sorted(samples)
    return ordered[math.ceil(fraction * len(ordered)) - 1]


def parse_mysql_status(output):
    """Parse MySQL's two-column, tab-separated status output."""
    return {
        name: int(value)
        for name, value in (line.split("\t") for line in output.splitlines())
    }


def mysql_status(defaults_file):
    """Read server-wide connection counters using credentials from a MySQL option file."""
    result = subprocess.run(
        [
            "mysql",
            f"--defaults-extra-file={defaults_file}",
            "--batch",
            "--skip-column-names",
            "-e",
            "SHOW GLOBAL STATUS WHERE Variable_name IN ('Connections', 'Threads_created')",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return parse_mysql_status(result.stdout)


def run_phase(url, clients, duration, authorization):
    """Send keep-alive GET requests from each client for one timed phase."""
    parsed = urlsplit(url)
    connection_type = (
        http.client.HTTPSConnection
        if parsed.scheme == "https"
        else http.client.HTTPConnection
    )
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    headers = {"Authorization": authorization} if authorization else {}
    deadline = time.monotonic() + duration

    def worker():
        latencies = []
        errors = 0
        connection = None
        try:
            while time.monotonic() < deadline:
                if connection is None:
                    connection = connection_type(
                        parsed.hostname, parsed.port, timeout=10
                    )
                start = time.perf_counter()
                try:
                    connection.request("GET", path, headers=headers)
                    response = connection.getresponse()
                    response.read()
                    elapsed_ms = (time.perf_counter() - start) * 1000
                    if 200 <= response.status < 300:
                        latencies.append(elapsed_ms)
                    else:
                        errors += 1
                except (OSError, http.client.HTTPException):
                    errors += 1
                    connection.close()
                    connection = None
        finally:
            if connection is not None:
                connection.close()
        return latencies, errors

    start = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=clients) as executor:
        results = list(executor.map(lambda _: worker(), range(clients)))
    elapsed = time.monotonic() - start
    latencies = [
        latency for client_latencies, _ in results for latency in client_latencies
    ]
    errors = sum(client_errors for _, client_errors in results)
    return {
        "requests": len(latencies),
        "errors": errors,
        "throughput_per_second": round(len(latencies) / elapsed, 1),
        "p50_ms": percentile(latencies, 0.50),
        "p99_ms": percentile(latencies, 0.99),
    }


def main():
    """Run warmup and measured rounds against an already-started Gravitino server."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--url",
        required=True,
        help="full URL of a GET route, such as http://localhost:8090/api/metalakes",
    )
    parser.add_argument("--mysql-defaults-extra-file", required=True)
    parser.add_argument("--clients", type=int, default=64)
    parser.add_argument("--warmup-seconds", type=float, default=3)
    parser.add_argument("--seconds", type=float, default=15)
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    if (
        args.clients < 1
        or args.warmup_seconds <= 0
        or args.seconds <= 0
        or args.rounds < 1
    ):
        parser.error("clients, warmup-seconds, seconds, and rounds must be positive")
    if urlsplit(args.url).scheme not in ("http", "https"):
        parser.error("url must use http or https")

    authorization = os.environ.get("GRAVITINO_BENCH_AUTHORIZATION")
    had_errors = False
    for round_number in range(1, args.rounds + 1):
        run_phase(args.url, args.clients, args.warmup_seconds, authorization)
        before = mysql_status(args.mysql_defaults_extra_file)
        result = run_phase(args.url, args.clients, args.seconds, authorization)
        after = mysql_status(args.mysql_defaults_extra_file)
        result["round"] = round_number
        had_errors |= result["errors"] > 0
        result["mysql_connections_delta"] = after["Connections"] - before["Connections"]
        result["mysql_threads_created_delta"] = (
            after["Threads_created"] - before["Threads_created"]
        )
        print(json.dumps(result, sort_keys=True), flush=True)
    if had_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
