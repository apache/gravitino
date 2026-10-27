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

import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from unittest.mock import patch

from jdbc_idle_retention import mysql_status, parse_mysql_status, percentile, run_phase


class TestJdbcIdleRetention(unittest.TestCase):
    def test_percentile(self):
        self.assertIsNone(percentile([], 0.99))
        self.assertEqual(1, percentile([4, 1, 3, 2], 0.25))
        self.assertEqual(4, percentile([4, 1, 3, 2], 0.99))

    def test_parse_mysql_status(self):
        self.assertEqual(
            {"Connections": 37, "Threads_created": 4},
            parse_mysql_status("Connections\t37\nThreads_created\t4\n"),
        )

    @patch("jdbc_idle_retention.subprocess.run")
    def test_mysql_status(self, run):
        run.return_value.stdout = "Connections\t37\nThreads_created\t4\n"
        self.assertEqual(
            {"Connections": 37, "Threads_created": 4}, mysql_status("/tmp/mysql.cnf")
        )
        self.assertEqual(
            "--defaults-extra-file=/tmp/mysql.cnf", run.call_args.args[0][1]
        )

    def test_run_phase(self):
        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *_args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            result = run_phase(
                f"http://127.0.0.1:{server.server_port}/api/metalakes", 4, 0.5, None
            )
            self.assertGreater(result["requests"], 0)
            self.assertEqual(0, result["errors"])
            self.assertGreaterEqual(result["p99_ms"], 0)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
