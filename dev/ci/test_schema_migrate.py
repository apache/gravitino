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

"""Regression tests for chart schema-migrate.sh (upgrade chain + fresh install)."""

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import time
import unittest
import uuid


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "charts/gravitino/resources/scripts/schema-migrate.sh"
SQL_ROOT = ROOT.parent / "scripts"
ENGINE = "mysql"
TARGET = "2.0.0"


def docker(*args, check=True):
    result = subprocess.run(
        ["docker", *args], text=True, capture_output=True, timeout=180
    )
    if check and result.returncode:
        raise RuntimeError(f"docker {args[0]} failed: {result.stderr or result.stdout}")
    return result


class TestSchemaMigrate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not SCRIPT.is_file():
            raise unittest.SkipTest(f"missing {SCRIPT}")
        sql_dir = SQL_ROOT / ENGINE
        if not sql_dir.is_dir():
            raise unittest.SkipTest(f"missing {sql_dir}")

        workspace = Path(os.environ.get("GITHUB_WORKSPACE") or ROOT.parent).resolve()
        cls.work = workspace / f".schema-migrate-test-{uuid.uuid4().hex[:12]}"
        cls.work.mkdir(parents=True)
        shutil.copyfile(SCRIPT, cls.work / "schema-migrate.sh")
        shutil.copytree(sql_dir, cls.work / ENGINE)
        cls.container = "schema-migrate-" + uuid.uuid4().hex[:12]
        cls.addClassCleanup(shutil.rmtree, cls.work, True)
        cls.addClassCleanup(docker, "rm", "-f", "-v", cls.container, check=False)

        image = "mysql:8.0" if ENGINE == "mysql" else "postgres:13"
        settings = (
            [
                "-e",
                "MYSQL_ROOT_PASSWORD=runtime-test-password",
                "-e",
                "MYSQL_INITDB_SKIP_TZINFO=1",
            ]
            if ENGINE == "mysql"
            else [
                "-e",
                "POSTGRES_HOST_AUTH_METHOD=trust",
                "-e",
                "POSTGRES_INITDB_ARGS=--no-sync",
            ]
        )
        docker(
            "run",
            "-d",
            "--network",
            "none",
            "--tmpfs",
            (
                "/var/lib/mysql:rw,size=512m"
                if ENGINE == "mysql"
                else "/var/lib/postgresql/data:rw,size=256m"
            ),
            "--name",
            cls.container,
            "-v",
            f"{cls.work}:/test:ro",
            *settings,
            image,
        )
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if (
                cls.sql(
                    "mysql" if ENGINE == "mysql" else "postgres",
                    "SELECT 1",
                    check=False,
                ).returncode
                == 0
            ):
                return
            time.sleep(1)
        raise RuntimeError(docker("logs", cls.container).stdout)

    @classmethod
    def sql(cls, database, sql, check=True):
        if ENGINE == "postgresql":
            client = [
                "psql",
                "-U",
                "postgres",
                "-d",
                database,
                "-v",
                "ON_ERROR_STOP=1",
                "-Atc",
                sql,
            ]
        else:
            client = ["mysql", "-uroot", "-D", database, "-sNe", sql]
        return docker(
            "exec",
            "-e",
            "MYSQL_PWD=runtime-test-password",
            cls.container,
            *client,
            check=check,
        )

    def setUp(self):
        self.database = "migrate_" + uuid.uuid4().hex[:12]
        self.sql(
            "mysql" if ENGINE == "mysql" else "postgres",
            f"CREATE DATABASE {self.database}",
        )
        self.scripts = self.work / self.database
        self.scripts.mkdir(parents=True)
        # Bind-mounted work is read-only in the container; copy scripts per DB.
        # The migrate helper writes version.txt next to the engine SQL tree.
        shutil.copytree(self.work / ENGINE, self.scripts / ENGINE)

    def write_version(self, target=TARGET):
        (self.scripts / "version.txt").write_text(target + "\n")

    def migrate(self, target=TARGET, baseline="", check=True):
        self.write_version(target)
        env = [
            "-e",
            f"DB_ENGINE={ENGINE}",
            "-e",
            "DB_HOST=127.0.0.1",
            "-e",
            f"GRAVITINO_DB={self.database}",
            "-e",
            f"SCRIPTS_DIR=/test/{self.database}",
            "-e",
            "MYSQL_USER=root",
            "-e",
            "MYSQL_PWD=runtime-test-password",
            "-e",
            "POSTGRES_USER=postgres",
            "-e",
            "POSTGRES_PASSWORD=runtime-test-password",
            "-e",
            f"BASELINE_VERSION={baseline}",
        ]
        # work dir is mounted read-only; copy scripts into a writable path first.
        docker(
            "exec",
            self.container,
            "bash",
            "-c",
            f"rm -rf /tmp/{self.database} && cp -a /test/{self.database} /tmp/{self.database}",
        )
        result = docker(
            "exec",
            *env,
            "-e",
            f"SCRIPTS_DIR=/tmp/{self.database}",
            self.container,
            "bash",
            "/test/schema-migrate.sh",
            check=check,
        )
        return result

    def seed_schema_file(self, version="1.2.0"):
        schema = self.scripts / ENGINE / f"schema-{version}-{ENGINE}.sql"
        self.assertTrue(schema.is_file(), f"missing {schema}")
        docker(
            "exec",
            "-e",
            "MYSQL_PWD=runtime-test-password",
            self.container,
            "bash",
            "-c",
            f"mysql -uroot -D {self.database} < /test/{self.database}/{ENGINE}/schema-{version}-{ENGINE}.sql",
        )

    def assert_version(self, version):
        self.assertEqual(
            self.sql(
                self.database,
                "SELECT schema_version FROM gravitino_schema_migration WHERE id=1",
            ).stdout.strip(),
            version,
        )

    def assert_view_meta_has_audit_info(self):
        scope = "DATABASE()" if ENGINE == "mysql" else "current_schema()"
        self.assertEqual(
            self.sql(
                self.database,
                "SELECT COUNT(*) FROM information_schema.columns "
                f"WHERE table_schema={scope} AND table_name='view_meta' "
                "AND column_name='audit_info'",
            ).stdout.strip(),
            "1",
        )

    def assert_view_meta_missing_audit_info(self):
        scope = "DATABASE()" if ENGINE == "mysql" else "current_schema()"
        self.assertEqual(
            self.sql(
                self.database,
                "SELECT COUNT(*) FROM information_schema.columns "
                f"WHERE table_schema={scope} AND table_name='view_meta' "
                "AND column_name='audit_info'",
            ).stdout.strip(),
            "0",
        )

    def test_fresh_install_records_target_version(self):
        result = self.migrate()
        self.assertIn("Fresh install detected", result.stdout)
        self.assert_version(TARGET)
        self.assert_view_meta_has_audit_info()

    def test_existing_1_2_0_applies_full_upgrade_chain(self):
        self.seed_schema_file("1.2.0")
        self.assert_view_meta_missing_audit_info()
        result = self.migrate(baseline="1.2.0")
        self.assertIn("upgrade-1.2.0-to-1.3.0", result.stdout)
        self.assertIn("upgrade-1.3.0-to-2.0.0", result.stdout)
        self.assertNotIn("tail -n 1", result.stdout)
        self.assert_version(TARGET)
        self.assert_view_meta_has_audit_info()

    def test_tail_n_1_would_skip_intermediate_is_rejected_by_chain(self):
        """If only the last upgrade ran, view_meta.audit_info would stay missing."""
        self.seed_schema_file("1.2.0")
        # Apply only the last upgrade (the old buggy chart behavior).
        docker(
            "exec",
            "-e",
            "MYSQL_PWD=runtime-test-password",
            self.container,
            "bash",
            "-c",
            f"mysql -uroot -D {self.database} < /test/{self.database}/{ENGINE}/upgrade-1.3.0-to-2.0.0-{ENGINE}.sql || true",
        )
        self.assert_view_meta_missing_audit_info()
        result = self.migrate(baseline="1.2.0")
        self.assertIn("upgrade-1.2.0-to-1.3.0", result.stdout)
        self.assert_view_meta_has_audit_info()
        self.assert_version(TARGET)

    def test_infer_1_3_0_keeps_version_capture_clean(self):
        """Ordinary 1.3.0 seed without baseline: infer a clean X.Y.Z and upgrade.

        A real schema-1.3.0.sql fixture lacks newer snapshot tables, so
        infer_schema_version never reaches the ambiguous-column INFO branch.
        That stderr diagnostic is covered by the script redirect itself; this
        case only asserts ordinary inference + a clean recorded version token.
        """
        self.seed_schema_file("1.3.0")
        result = self.migrate(baseline="")
        self.assertIn("inferred 1.3.0 from existing tables", result.stdout)
        self.assertIn("upgrade-1.3.0-to-2.0.0", result.stdout)
        # Inference / logging must not leak diagnostic text into stdout capture.
        self.assertNotIn("INFO: Tables also match schema-", result.stdout)
        self.assertRegex(
            result.stdout,
            r"No recorded schema version; inferred 1\.3\.0 from existing tables\.",
        )
        # Recorded versions during the chain must be clean X.Y.Z tokens only.
        recorded = self.sql(
            self.database,
            "SELECT schema_version FROM gravitino_schema_migration WHERE id=1",
        ).stdout.strip()
        self.assertRegex(recorded, r"^[0-9]+\.[0-9]+\.[0-9]+$")
        self.assert_version(TARGET)
        self.assertEqual(
            self.sql(
                self.database,
                "SELECT COUNT(*) FROM information_schema.columns "
                f"WHERE table_schema=DATABASE() AND table_name='idp_user_meta' "
                "AND column_name='enabled'",
            ).stdout.strip(),
            "1",
        )


class TestSchemaMigratePostgresCurrentSchema(unittest.TestCase):
    """Fresh target schema must not skip init when public already has metalake_meta."""

    @classmethod
    def setUpClass(cls):
        if not SCRIPT.is_file():
            raise unittest.SkipTest(f"missing {SCRIPT}")
        sql_dir = SQL_ROOT / "postgresql"
        if not sql_dir.is_dir():
            raise unittest.SkipTest(f"missing {sql_dir}")

        workspace = Path(os.environ.get("GITHUB_WORKSPACE") or ROOT.parent).resolve()
        cls.work = workspace / f".schema-migrate-pg-test-{uuid.uuid4().hex[:12]}"
        cls.work.mkdir(parents=True)
        shutil.copyfile(SCRIPT, cls.work / "schema-migrate.sh")
        shutil.copytree(sql_dir, cls.work / "postgresql")
        cls.container = "schema-migrate-pg-" + uuid.uuid4().hex[:12]
        cls.addClassCleanup(shutil.rmtree, cls.work, True)
        cls.addClassCleanup(docker, "rm", "-f", "-v", cls.container, check=False)

        docker(
            "run",
            "-d",
            "--network",
            "none",
            "--tmpfs",
            "/var/lib/postgresql/data:rw,size=256m",
            "--name",
            cls.container,
            "-e",
            "POSTGRES_HOST_AUTH_METHOD=trust",
            "-e",
            "POSTGRES_INITDB_ARGS=--no-sync",
            "-v",
            f"{cls.work}:/test:ro",
            "postgres:13",
        )
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if (
                docker(
                    "exec",
                    cls.container,
                    "psql",
                    "-U",
                    "postgres",
                    "-d",
                    "postgres",
                    "-c",
                    "SELECT 1",
                    check=False,
                ).returncode
                == 0
            ):
                return
            time.sleep(1)
        raise RuntimeError(docker("logs", cls.container).stdout)

    def test_fresh_target_schema_ignores_public_metalake(self):
        db = "migrate_pg_" + uuid.uuid4().hex[:8]
        target = "gravitino_app"
        docker(
            "exec",
            self.container,
            "psql",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            f"CREATE DATABASE {db}",
        )
        # public already has a catalog; target schema is empty.
        docker(
            "exec",
            self.container,
            "psql",
            "-U",
            "postgres",
            "-d",
            db,
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "CREATE TABLE public.metalake_meta (id int); "
            f"CREATE SCHEMA {target};",
        )

        scripts = self.work / db
        scripts.mkdir(parents=True)
        shutil.copytree(self.work / "postgresql", scripts / "postgresql")
        (scripts / "version.txt").write_text(TARGET + "\n")
        docker(
            "exec",
            self.container,
            "bash",
            "-c",
            f"rm -rf /tmp/{db} && cp -a /test/{db} /tmp/{db}",
        )

        result = docker(
            "exec",
            "-e",
            "DB_ENGINE=postgresql",
            "-e",
            "DB_HOST=127.0.0.1",
            "-e",
            f"GRAVITINO_DB={db}",
            "-e",
            f"SCRIPTS_DIR=/tmp/{db}",
            "-e",
            "POSTGRES_USER=postgres",
            "-e",
            "POSTGRES_PASSWORD=runtime-test-password",
            "-e",
            f"PG_CURRENT_SCHEMA={target}",
            "-e",
            f"PGOPTIONS=-c search_path={target}",
            self.container,
            "bash",
            "/test/schema-migrate.sh",
        )
        self.assertIn("Fresh install detected", result.stdout)
        self.assertIn("Schema migration completed successfully", result.stdout)

        # Target schema got the snapshot; public still only has the stub table.
        target_tables = docker(
            "exec",
            self.container,
            "psql",
            "-U",
            "postgres",
            "-d",
            db,
            "-Atc",
            "SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema='{target}' AND table_name='metalake_meta'",
        ).stdout.strip()
        self.assertEqual(target_tables, "1")
        recorded = docker(
            "exec",
            self.container,
            "psql",
            "-U",
            "postgres",
            "-d",
            db,
            "-Atc",
            f"SET search_path TO {target}; "
            "SELECT schema_version FROM gravitino_schema_migration WHERE id=1",
        ).stdout.strip()
        self.assertEqual(recorded, TARGET)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("mysql",), default="mysql")
    args, remaining = parser.parse_known_args()
    ENGINE = args.engine
    unittest.main(argv=[__file__, *remaining])
