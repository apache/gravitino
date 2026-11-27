# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Run packaged catalog regressions with a main archive and a standalone archive.

Usage (Python 3.12+ and JDK 17):
    python3 test_packaged_catalog.py MAIN_ARCHIVE STANDALONE_ARCHIVE
Cloud requests terminate at local HTTP fixtures. Each launcher uses extracted files.
"""
import copy
import fnmatch
import base64
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


ARCHIVES = []


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request(base, path, method="GET", data=None, token=None):
    auth = "Bearer " + token if token else "Basic " + base64.b64encode(b"admin:dummy").decode()
    req = urllib.request.Request(
        base + path, method=method,
        headers={"Authorization": auth, "Content-Type": "application/json"},
        data=None if data is None else json.dumps(data).encode())
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        with error:
            return error.code, error.read().decode()


def policy_allows(policy, action, resource, context):
    """Evaluate the IAM actions, resources and string conditions emitted by this test."""
    allowed = False
    for statement in policy.get("Statement", []):
        actions = statement["Action"]
        resources = statement["Resource"]
        if isinstance(actions, str):
            actions = [actions]
        if isinstance(resources, str):
            resources = [resources]
        if not any(fnmatch.fnmatchcase(action, pattern) for pattern in actions):
            continue
        if not any(fnmatch.fnmatchcase(resource, pattern) for pattern in resources):
            continue
        matches = True
        for operator, conditions in statement.get("Condition", {}).items():
            if operator not in ("StringLike", "StringEquals"):
                raise AssertionError(f"Unsupported policy condition {operator}")
            for key, values in conditions.items():
                if isinstance(values, str):
                    values = [values]
                value = context.get(key)
                if value is None:
                    matches = False
                elif operator == "StringEquals":
                    matches = matches and value in values
                else:
                    matches = matches and any(fnmatch.fnmatchcase(value, pattern) for pattern in values)
        if matches:
            if statement["Effect"] == "Deny":
                return False
            allowed = allowed or statement["Effect"] == "Allow"
    return allowed


class PackagedCatalogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if len(ARCHIVES) != 2:
            raise unittest.SkipTest("Supply the main and standalone distribution archives")
        cls.root = Path(tempfile.mkdtemp(prefix="paimon-packaged-regressions-"))
        print("RESULT_DIRECTORY", cls.root, flush=True)
        cls.processes = []
        cls.logs = []
        cls.addClassCleanup(cls.stop)
        cls.policies = []
        cls.policies_by_token = {}
        cls.storage_tokens = []
        cls.root_probes = 0

        class STS(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def authorized(self, action, prefix=None):
                token = self.headers.get("x-amz-security-token")
                policy = cls.policies_by_token.get(token, {})
                cls.storage_tokens.append(token)
                location = urllib.parse.urlsplit(self.path)
                bucket, _, key = location.path.lstrip("/").partition("/")
                context = {}
                if action == "s3:ListBucket":
                    resource = "arn:aws:s3:::" + bucket
                    context["s3:prefix"] = prefix if prefix is not None else urllib.parse.parse_qs(
                        location.query, keep_blank_values=True).get("prefix", [""])[0]
                else:
                    resource = "arn:aws:s3:::" + bucket + "/" + key
                if policy_allows(policy, action, resource, context):
                    return True
                self.send_response(403)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return False

            def do_HEAD(self):
                location = urllib.parse.urlsplit(self.path)
                bucket, _, key = location.path.lstrip("/").partition("/")
                if not key:
                    if not self.authorized("s3:ListBucket", ""):
                        return
                    status = 200
                else:
                    if not self.authorized("s3:GetObject"):
                        return
                    marker = key == "warehouse/" or key.endswith("/warehouse/")
                    if key == "warehouse" or key.endswith("/warehouse"):
                        cls.root_probes += 1
                    # Missing-key visibility is checked against the issued bucket-list policy.
                    if not marker and not self.authorized("s3:ListBucket", key):
                        return
                    status = 200 if marker else 404
                self.send_response(status)
                self.send_header("Content-Length", "0")
                self.send_header("Last-Modified", "Thu, 26 Nov 2026 00:00:00 GMT")
                self.send_header("ETag", '"test"')
                self.end_headers()

            def do_GET(self):
                location = urllib.parse.urlsplit(self.path)
                action = "s3:ListBucket" if location.query else "s3:GetObject"
                if not self.authorized(action):
                    return
                payload = (b"<ListBucketResult><Name>bucket</Name><Prefix>warehouse/</Prefix>"
                           b"<KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys>"
                           b"<IsTruncated>false</IsTruncated></ListBucketResult>")
                self.send_response(200)
                self.send_header("Content-Type", "application/xml")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_PUT(self):
                if not self.authorized("s3:PutObject"):
                    return
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                self.send_response(200)
                self.send_header("ETag", '"test"')
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"])).decode()
                policy = json.loads(urllib.parse.parse_qs(body)["Policy"][0])
                cls.policies.append(policy)
                token = "session-token-" + str(len(cls.policies))
                cls.policies_by_token[token] = policy
                expiry = (datetime.now(timezone.utc) + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
                payload = (f'<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
                           f'<AssumeRoleResult><Credentials><AccessKeyId>session-id</AccessKeyId>'
                           f'<SecretAccessKey>session-key</SecretAccessKey><SessionToken>{token}</SessionToken>'
                           f'<Expiration>{expiry}</Expiration></Credentials>'
                           f'<AssumedRoleUser><Arn>arn:aws:sts::123456789012:assumed-role/test/session</Arn>'
                           f'<AssumedRoleId>test:session</AssumedRoleId></AssumedRoleUser>'
                           f'</AssumeRoleResult><ResponseMetadata><RequestId>test</RequestId>'
                           f'</ResponseMetadata></AssumeRoleResponse>').encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/xml")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        cls.sts = ThreadingHTTPServer(("127.0.0.1", 0), STS)
        cls.thread = threading.Thread(target=cls.sts.serve_forever, daemon=True)
        cls.thread.start()
        cls.addClassCleanup(cls.sts.server_close)
        cls.addClassCleanup(cls.sts.shutdown)
        homes = []
        for index, archive in enumerate(ARCHIVES):
            dest = cls.root / str(index)
            with tarfile.open(archive, "r:gz") as package:
                for member in package:
                    rel = "/".join(Path(member.name).parts[1:])
                    if rel in ("LICENSE", "NOTICE") or rel.startswith(
                            ("licenses/", "libs/", "bin/", "scripts/", "conf/", "paimon-rest-server/",
                             "catalogs/lakehouse-paimon/", "authorizations/", "auxlib/")):
                        package.extract(member, dest, filter="data")
            homes.append(next(dest.iterdir()))
        cls.home = homes[0]
        main_port, aux_port, standalone_port = free_port(), free_port(), free_port()
        cls.main = f"http://127.0.0.1:{main_port}"
        cls.rest = f"http://127.0.0.1:{aux_port}/paimon/"
        cls.remote = f"http://127.0.0.1:{standalone_port}/paimon/"
        config = cls.home / "conf/gravitino.conf"
        with config.open("a") as out:
            out.write("\n" + "\n".join([
                "gravitino.server.webserver.host=127.0.0.1",
                f"gravitino.server.webserver.httpPort={main_port}",
                "gravitino.authenticators=simple", "gravitino.authorization.enable=true",
                "gravitino.authorization.serviceAdmins=admin",
                "gravitino.auxService.names=paimon-rest",
                "gravitino.paimon-rest.classpath=paimon-rest-server/libs,paimon-rest-server/conf",
                "gravitino.paimon-rest.host=127.0.0.1", f"gravitino.paimon-rest.httpPort={aux_port}",
                "gravitino.paimon-rest.catalog-config-provider=dynamic-config-provider",
                "gravitino.paimon-rest.gravitino-metalake=ml",
                "gravitino.paimon-rest.token.admin=admin-token",
                "gravitino.paimon-rest.data-token.enabled=true"]) + "\n")
        cls.launch(cls.home, "gravitino.sh", cls.main + "/api/health/ready")
        cls.api("/api/metalakes", "POST", {"name": "ml", "properties": {}})
        cls.api("/api/metalakes/ml/catalogs", "POST", {
            "name": "default_catalog", "type": "RELATIONAL", "provider": "lakehouse-paimon",
            "properties": {"catalog-backend": "filesystem",
                           "warehouse": (cls.home / "warehouse").as_uri()}})
        standalone = homes[1]
        (standalone / "conf/gravitino-paimon-rest-server.conf").write_text("\n".join([
            "gravitino.paimon-rest.host=127.0.0.1",
            f"gravitino.paimon-rest.httpPort={standalone_port}",
            "gravitino.paimon-rest.catalog-config-provider=dynamic-config-provider",
            f"gravitino.paimon-rest.gravitino-uri={cls.main}",
            "gravitino.paimon-rest.gravitino-metalake=ml",
            "gravitino.paimon-rest.gravitino-simple.user-name=admin",
            "gravitino.paimon-rest.data-token.enabled=true",
            "gravitino.paimon-rest.token=admin-token"]) + "\n")
        cls.launch(standalone, "gravitino-paimon-rest-server.sh", cls.remote + "health/ready")

    @classmethod
    def launch(cls, home, launcher, ready):
        env = dict(os.environ)
        env.update(GRAVITINO_HOME=str(home), GRAVITINO_CONF_DIR=str(home / "conf"),
                   GRAVITINO_LOG_DIR=str(home / "logs"),
                   GRAVITINO_MEM="-Xms128m -Xmx768m -XX:MaxMetaspaceSize=384m")
        log = (home / "launcher.log").open("w")
        cls.logs.append(log)
        process = subprocess.Popen(["bash", str(home / "bin" / launcher), "run"],
                                   cwd=home, env=env, stdout=log, stderr=subprocess.STDOUT)
        cls.processes.append(process)
        for attempt in range(150):
            if process.poll() is not None:
                raise RuntimeError(f"Startup failed, inspect {home / 'launcher.log'}")
            try:
                with urllib.request.urlopen(ready, timeout=1) as response:
                    if response.status == 200:
                        return
            except (urllib.error.URLError, TimeoutError):
                time.sleep(.2)
        raise RuntimeError(f"Readiness timed out for {ready}")

    @classmethod
    def stop(cls):
        for process in reversed(cls.processes):
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        for log in cls.logs:
            log.close()

    @classmethod
    def api(cls, path, method="GET", data=None):
        status, body = request(cls.main, path, method, data)
        if status != 200:
            raise AssertionError((status, body))
        return json.loads(body)

    def rest_call(self, path, method="GET", data=None, base=None):
        return request(base or self.rest, "v1/" + path, method, data, "admin-token")

    def create_table(self, database, table):
        self.api("/api/metalakes/ml/catalogs/default_catalog/schemas/" + database + "/tables",
                 "POST", {"name": table, "columns": [{"name": "id", "type": "integer",
                                                    "nullable": True}], "properties": {}})

    def rename(self, source_db, source, target_db, target):
        return self.rest_call("default_catalog/tables/rename", "POST", {
            "source": {"database": source_db, "object": source},
            "destination": {"database": target_db, "object": target}})

    def test_01_register_sts_catalogs_via_main_api(self):
        for scheme in ("s3", "oss"):
            properties = {"catalog-backend": "filesystem",
                          "warehouse": scheme + "://bucket/warehouse",
                          "credential-providers": scheme + "-token"}
            if scheme == "s3":
                properties.update({"s3-access-key-id": "source-id",
                    "s3-secret-access-key": "source-key", "s3-region": "us-east-1",
                    "s3-role-arn": "arn:aws:iam::123456789012:role/vending",
                    "s3-token-expire-in-secs": "7200",
                    "s3-token-service-endpoint": f"http://127.0.0.1:{self.sts.server_port}",
                    "s3-endpoint": f"http://127.0.0.1:{self.sts.server_port}",
                    "gravitino.bypass.s3.path.style.access": "true"})
            self.api("/api/metalakes/ml/catalogs", "POST", {
                "name": "sts_" + scheme, "type": "RELATIONAL",
                "provider": "lakehouse-paimon", "properties": properties})
        for base in (self.rest, self.remote):
            status, body = self.rest_call("config?warehouse=sts_s3", base=base)
            self.assertEqual(200, status, body)
            status, body = self.rest_call("sts_s3/databases/db/tables/t/token", base=base)
            self.assertEqual(200, status, (base, body))
            self.assertIn("session-token", body)
            self.assertIn("session-key", body)
            self.assertNotIn("source-key", body)
        self.assertGreaterEqual(self.root_probes, 2)
        self.assertTrue(all(token in self.policies_by_token for token in self.storage_tokens))
        self.assertEqual(2, len(self.policies))
        for policy in self.policies:
            policy_json = json.dumps(policy)
            self.assertIn("bucket/warehouse/*", policy_json)
            self.assertIn("s3:PutObject", policy_json)
            self.assertNotIn("arn:aws:s3:::bucket/*", policy_json)

        token = next(iter(self.policies_by_token))
        storage = f"http://127.0.0.1:{self.sts.server_port}"
        for method, path in (
                ("GET", "/bucket/warehouse-other/data"),
                ("PUT", "/bucket/warehouse-other/data"),
                ("GET", "/bucket?list-type=2&prefix=warehouse-other/")):
            req = urllib.request.Request(storage + path, method=method,
                headers={"x-amz-security-token": token},
                data=b"payload" if method == "PUT" else None)
            with self.assertRaises(urllib.error.HTTPError) as caught:
                urllib.request.urlopen(req, timeout=5)
            with caught.exception as error:
                self.assertEqual(403, error.code)
        policy = self.policies_by_token[token]
        self.assertTrue(policy_allows(policy, "s3:GetObject", "arn:aws:s3:::bucket/warehouse", {}))
        self.assertFalse(policy_allows(policy, "s3:PutObject", "arn:aws:s3:::bucket/warehouse", {}))
        self.assertFalse(policy_allows(policy, "s3:ListBucket", "arn:aws:s3:::bucket",
                                      {"s3:prefix": "warehouse-other/"}))

        missing_root = copy.deepcopy(policy)
        for statement in missing_root["Statement"]:
            resources = statement["Resource"]
            if isinstance(resources, list):
                statement["Resource"] = [
                    resource for resource in resources
                    if resource != "arn:aws:s3:::bucket/warehouse"]
        self.policies_by_token["missing-root"] = missing_root
        explicitly_denied = copy.deepcopy(policy)
        explicitly_denied["Statement"].append({
            "Effect": "Deny", "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::bucket/warehouse"})
        self.policies_by_token["explicit-deny"] = explicitly_denied
        no_list = copy.deepcopy(policy)
        no_list["Statement"] = [
            statement for statement in no_list["Statement"]
            if "s3:ListBucket" not in statement["Action"]]
        self.policies_by_token["no-list"] = no_list
        denied_list = copy.deepcopy(policy)
        denied_list["Statement"].append({
            "Effect": "Deny", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::bucket"})
        self.policies_by_token["denied-list"] = denied_list
        wrong_prefix = copy.deepcopy(policy)
        for statement in wrong_prefix["Statement"]:
            if "s3:ListBucket" in statement["Action"]:
                statement["Condition"] = {"StringLike": {"s3:prefix": "elsewhere/*"}}
        self.policies_by_token["wrong-prefix"] = wrong_prefix
        for denied_token in ("unknown-token", "missing-root", "explicit-deny",
                             "no-list", "denied-list", "wrong-prefix"):
            req = urllib.request.Request(storage + "/bucket/warehouse", method="HEAD",
                                         headers={"x-amz-security-token": denied_token})
            with self.assertRaises(urllib.error.HTTPError) as caught:
                urllib.request.urlopen(req, timeout=5)
            with caught.exception as error:
                self.assertEqual(403, error.code)

    def test_01_z_bucket_root_policy_lists_descendants(self):
        for index, warehouse in enumerate(("s3://bucket", "s3://bucket/")):
            name = "bucket_root_" + str(index)
            properties = {
                "catalog-backend": "filesystem", "warehouse": warehouse,
                "credential-providers": "s3-token", "s3-access-key-id": "source-id",
                "s3-secret-access-key": "source-key", "s3-region": "us-east-1",
                "s3-role-arn": "arn:aws:iam::123456789012:role/vending",
                "s3-token-expire-in-secs": "7200",
                "s3-token-service-endpoint": f"http://127.0.0.1:{self.sts.server_port}",
                "s3-endpoint": f"http://127.0.0.1:{self.sts.server_port}",
                "gravitino.bypass.s3.path.style.access": "true"}
            self.api("/api/metalakes/ml/catalogs", "POST", {
                "name": name, "type": "RELATIONAL", "provider": "lakehouse-paimon",
                "properties": properties})
            for base in (self.rest, self.remote):
                status, body = self.rest_call("config?warehouse=" + name, base=base)
                self.assertEqual(200, status, body)
                status, body = self.rest_call(name + "/databases/db/tables/t/token", base=base)
                self.assertEqual(200, status, body)
                token = json.loads(body)["token"]["s3.session-token"]
                for prefix in ("", "db.db/", "db.db/table/"):
                    url = (f"http://127.0.0.1:{self.sts.server_port}/bucket?list-type=2&"
                           + urllib.parse.urlencode({"prefix": prefix}))
                    req = urllib.request.Request(url, headers={"x-amz-security-token": token})
                    with urllib.request.urlopen(req, timeout=5) as response:
                        self.assertEqual(200, response.status)

    def test_02_schema_creation_is_validated_before_native_changes(self):
        for length in (128, 129):
            name = "s" * length
            status, body = self.rest_call("default_catalog/databases", "POST",
                                          {"name": name, "options": {}})
            self.assertEqual(200 if length == 128 else 400, status, body)
            self.assertEqual(length == 128, (self.home / ("warehouse/" + name + ".db")).exists())
            if length == 129:
                self.assertEqual(400, self.rest_call("default_catalog/databases", "POST",
                                                   {"name": name, "options": {}})[0])
        name = "stale_schema"
        status, body = self.rest_call("default_catalog/databases", "POST",
                                      {"name": name, "options": {}})
        self.assertEqual(200, status, body)
        self.create_table(name, "source")
        path = "default_catalog/databases/" + name + "/tables/source"
        status, body = self.rest_call(path)
        table_id = json.loads(body)["id"]
        tags = "/api/metalakes/ml/objects/schema/default_catalog." + name + "/tags"
        owner = "/api/metalakes/ml/owners/schema/default_catalog." + name
        old_owner = self.api(owner)
        self.api("/api/metalakes/ml/tags", "POST", {"name": "schema_tag", "properties": {}})
        self.api(tags, "POST", {"tagsToAdd": ["schema_tag"], "tagsToRemove": []})
        location = self.home / ("warehouse/" + name + ".db")
        backup = self.home / "stale-schema-backup"
        location.rename(backup)
        try:
            for attempt in range(2):
                status, body = self.rest_call("default_catalog/databases", "POST",
                                              {"name": name, "options": {}})
                self.assertEqual(409, status, body)
                self.assertFalse(location.exists())
        finally:
            backup.rename(location)
        self.assertEqual(["schema_tag"], self.api(tags)["names"])
        self.assertEqual(old_owner, self.api(owner))
        self.assertEqual(table_id, json.loads(self.rest_call(path)[1])["id"])

    def test_02_ddl_failure_and_column_identity(self):
        status, body = self.rest_call("default_catalog/databases", "POST", {"name": "ddl", "options": {}})
        self.assertEqual(200, status, body)
        self.create_table("ddl", "source")
        status, body = self.rest_call("default_catalog/databases/ddl/tables/source")
        self.assertEqual(200, status, body)
        schema = json.loads(body)["schema"]
        warehouse = self.home / "warehouse/ddl.db"
        for attempt in range(2):
            long_name = "a" * 129
            status, body = self.rest_call("default_catalog/databases/ddl/tables", "POST", {
                "identifier": {"database": "ddl", "object": long_name}, "schema": schema})
            self.assertEqual(400, status, body)
            self.assertFalse((warehouse / long_name).exists())
        status, body = self.rename("ddl", "source", "ddl", "b" * 129)
        self.assertEqual(400, status, body)
        self.assertTrue((warehouse / "source").exists())
        self.assertEqual(404, self.rename("absent", "source", "ddl", "target")[0])
        status, body = self.rest_call("default_catalog/databases/ddl/tables/absent", "POST", {
            "changes": [{"action": "renameColumn", "fieldNames": ["id"], "newName": "new_id"}]})
        self.assertEqual(404, status, body)
        self.create_table("ddl", "stale")
        (warehouse / "stale").rename(self.home / "stale-backup")
        status, body = self.rename("ddl", "source", "ddl", "stale")
        self.assertEqual(409, status, body)
        self.assertTrue((warehouse / "source").exists())
        self.assertFalse((warehouse / "stale").exists())
        self.api("/api/metalakes/ml/tags", "POST", {"name": "keep", "properties": {}})
        tags = "/api/metalakes/ml/objects/column/default_catalog.ddl.source."
        self.api(tags + "id/tags", "POST", {"tagsToAdd": ["keep"], "tagsToRemove": []})
        status, body = self.rest_call("default_catalog/databases/ddl/tables/source", "POST", {
            "changes": [{"action": "renameColumn", "fieldNames": ["id"], "newName": "new_id"}]})
        self.assertEqual(200, status, body)
        table_path = "/api/metalakes/ml/catalogs/default_catalog/schemas/ddl/tables/source"
        self.assertEqual("new_id", self.api(table_path)["table"]["columns"][0]["name"])
        self.assertEqual(["keep"], self.api(tags + "new_id/tags")["names"])
        self.api("/api/metalakes/ml/catalogs/default_catalog", "PUT", {
            "updates": [{"@type": "updateComment", "newComment": "reload"}]})
        self.assertEqual(["keep"], self.api(tags + "new_id/tags")["names"])

    def test_03_validate_columns_before_native_changes(self):
        self.assertEqual(200, self.rest_call("default_catalog/databases", "POST",
                                          {"name": "validation", "options": {}})[0])
        self.create_table("validation", "source")
        path = "default_catalog/databases/validation/tables"
        status, body = self.rest_call(path + "/source")
        original = json.loads(body)
        bad = copy.deepcopy(original["schema"])
        bad["fields"][0]["name"] = "c" * 301
        for attempt in range(2):
            status, body = self.rest_call(path, "POST", {
                "identifier": {"database": "validation", "object": "bad"}, "schema": bad})
            self.assertEqual(400, status, body)
            self.assertFalse((self.home / "warehouse/validation.db/bad").exists())
        status, body = self.rest_call(path + "/source", "POST", {
            "changes": [
                {"action": "renameColumn", "fieldNames": ["id"], "newName": "valid"},
                {"action": "renameColumn", "fieldNames": ["valid"], "newName": "c" * 301}]})
        self.assertEqual(400, status, body)
        status, body = self.rest_call(path + "/source")
        self.assertEqual(original, json.loads(body))
        main_path = "/api/metalakes/ml/catalogs/default_catalog/schemas/validation/tables/source"
        self.assertEqual("id", self.api(main_path)["table"]["columns"][0]["name"])

    def test_04_stale_creation_preserves_old_identity(self):
        self.assertEqual(200, self.rest_call("default_catalog/databases", "POST",
                                          {"name": "stale", "options": {}})[0])
        self.create_table("stale", "source")
        path = "default_catalog/databases/stale/tables/source"
        status, body = self.rest_call(path)
        original = json.loads(body)
        tags = "/api/metalakes/ml/objects/table/default_catalog.stale.source/tags"
        self.api("/api/metalakes/ml/tags", "POST", {"name": "old_tag", "properties": {}})
        self.api(tags, "POST", {"tagsToAdd": ["old_tag"], "tagsToRemove": []})
        owner_path = "/api/metalakes/ml/owners/table/default_catalog.stale.source"
        owner = self.api(owner_path)
        location = self.home / "warehouse/stale.db/source"
        backup = self.home / "stale-source-backup"
        location.rename(backup)
        try:
            for attempt in range(2):
                status, body = self.rest_call("default_catalog/databases/stale/tables", "POST", {
                    "identifier": {"database": "stale", "object": "source"},
                    "schema": original["schema"]})
                self.assertEqual(409, status, body)
                self.assertFalse(location.exists())
        finally:
            backup.rename(location)
        self.assertEqual(["old_tag"], self.api(tags)["names"])
        self.assertEqual(owner, self.api(owner_path))
        status, body = self.rest_call(path)
        self.assertEqual(original["id"], json.loads(body)["id"])

    def test_05_clone_schema_and_protect_identity_options(self):
        self.assertEqual(200, self.rest_call("default_catalog/databases", "POST",
                                          {"name": "identity", "options": {}})[0])
        self.create_table("identity", "source")
        path = "default_catalog/databases/identity/tables"
        status, body = self.rest_call(path + "/source")
        original = json.loads(body)
        self.assertNotIn("gravitino.identifier", original["schema"]["options"])
        status, body = self.rest_call(path, "POST", {
            "identifier": {"database": "identity", "object": "clone"},
            "schema": original["schema"]})
        self.assertEqual(200, status, body)
        status, body = self.rest_call(path + "/clone")
        self.assertNotEqual(original["id"], json.loads(body)["id"])
        source_table = "/api/metalakes/ml/catalogs/default_catalog/schemas/identity/tables/source"
        self.api(source_table)
        for key in ("gravitino.identifier", "rest.uuid"):
            schema = copy.deepcopy(original["schema"])
            schema["options"][key] = "untrusted-id"
            status, body = self.rest_call(path, "POST", {
                "identifier": {"database": "identity", "object": "bad_identity"}, "schema": schema})
            self.assertEqual(400, status, body)
            self.assertFalse((self.home / "warehouse/identity.db/bad_identity").exists())
            for action in ("setOption", "removeOption"):
                change = {"action": action, "key": key}
                if action == "setOption":
                    change["value"] = "untrusted-id"
                status, body = self.rest_call(path + "/source", "POST", {"changes": [change]})
                self.assertEqual(400, status, body)
            status, body = self.rest_call(path + "/source")
            self.assertEqual(original["id"], json.loads(body)["id"])
            self.assertNotIn("gravitino.identifier", json.loads(body)["schema"]["options"])

    def test_03_oss_fileio_in_both_archives(self):
        requests = []

        class Storage(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_HEAD(self):
                requests.append("HEAD")
                self.send_response(404)
                self.end_headers()

            def do_GET(self):
                requests.append("GET")
                body = (b"<ListBucketResult><Name>test-bucket</Name><Prefix></Prefix>"
                        b"<KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys>"
                        b"<IsTruncated>false</IsTruncated></ListBucketResult>")
                self.send_response(200)
                self.send_header("Content-Type", "application/xml")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        storage = ThreadingHTTPServer(("127.0.0.1", 0), Storage)
        thread = threading.Thread(target=storage.serve_forever, daemon=True)
        thread.start()
        try:
            source = self.root / "CheckOss.java"
            source.write_text("""
import java.util.Map;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
class CheckOss {
  public static void main(String[] args) throws Exception {
    Options options = Options.fromMap(Map.of(
        "fs.oss.accessKeyId", "test-key", "fs.oss.accessKeySecret", "test-secret",
        "fs.oss.endpoint", args[0]));
    Path path = new Path("oss://test-bucket/warehouse");
    try (FileIO io = FileIO.get(path, CatalogContext.create(options))) {
      if (!io.isObjectStore() || io.exists(path)) {
        throw new AssertionError("Expected OSS FileIO against the local missing-object fixture");
      }
    }
  }
}
""")
            for home in (self.home, next((self.root / "1").iterdir())):
                libs = home / ("paimon-rest-server/libs" if home == self.home else "libs")
                requests.clear()
                result = subprocess.run(
                    ["java", "-cp", str(libs / "*"), str(source),
                     f"http://127.0.0.1:{storage.server_port}"],
                    capture_output=True, text=True, timeout=60)
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                self.assertIn("HEAD", requests)
                self.assertIn("GET", requests)
        finally:
            storage.shutdown()
            storage.server_close()
            thread.join(timeout=5)


if __name__ == "__main__":
    ARCHIVES = [Path(arg).resolve() for arg in sys.argv[1:]]
    if len(ARCHIVES) != 2:
        raise SystemExit(__doc__)
    unittest.main(argv=[sys.argv[0]], verbosity=2)
