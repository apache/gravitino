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
import shutil
import tempfile
import time
from pathlib import Path
from random import randint
from time import sleep

from gravitino import GravitinoAdminClient, GravitinoMetalake
from gravitino.api.job.job_handle import JobHandle
from gravitino.api.job.shell_job_template import ShellJobTemplate
from gravitino.exceptions.base import (
    JobTemplateAlreadyExistsException,
    NoSuchJobTemplateException,
    NoSuchJobException,
)
from tests.integration.integration_test_env import IntegrationTestEnv


class TestSupportsJobs(IntegrationTestEnv):
    _metalake_name: str = "job_it_metalake" + str(randint(0, 1000))

    _gravitino_admin_client: GravitinoAdminClient = None
    _metalake: GravitinoMetalake = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.test_staging_dir = tempfile.mkdtemp()
        cls.test_entry_script_path = cls.generate_test_entry_script()
        cls.test_lib_script_path = cls.generate_test_lib_script()

        cls.builder = (
            ShellJobTemplate.builder()
            .with_comment("Test shell job template")
            .with_executable(cls.test_entry_script_path)
            .with_arguments(["{{arg1}}", "{{arg2}}"])
            .with_environments({"ENV_VAR": "{{env_var}}"})
            .with_scripts([cls.test_lib_script_path])
            .with_custom_fields({})
        )
        cls.configs = {
            "gravitino.job.stagingDir": cls.test_staging_dir,
            "gravitino.job.statusPullIntervalInMs": "3000",
        }

        cls._get_gravitino_home()
        cls._append_conf(cls.configs, f"{cls.gravitino_home}/conf/gravitino.conf")
        cls.restart_server()

        cls._gravitino_admin_client = GravitinoAdminClient(uri="http://localhost:8090")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.test_staging_dir)

    def setUp(self):
        self._metalake = self._gravitino_admin_client.create_metalake(
            self._metalake_name, comment="test metalake", properties={}
        )

    def tearDown(self):
        self._gravitino_admin_client.drop_metalake(self._metalake_name, force=True)

    def test_register_and_list_job_templates(self):
        template_1 = self.builder.with_name("test_1").build()
        template_2 = self.builder.with_name("test_2").build()

        # Assert no exceptions are raised when registering job templates
        self.assertIsNone(self._metalake.register_job_template(template_1))
        self.assertIsNone(self._metalake.register_job_template(template_2))

        # List registered job templates
        registered_templates = self._metalake.list_job_templates()
        self.assertEqual(len(registered_templates), 2)
        self.assertIn(template_1, registered_templates)
        self.assertIn(template_2, registered_templates)

        # Test registering a duplicate job template
        with self.assertRaises(JobTemplateAlreadyExistsException):
            self._metalake.register_job_template(template_1)

    def test_register_and_get_job_template(self):
        template = self.builder.with_name("test_get").build()

        # Assert no exceptions are raised when registering the job template
        self.assertIsNone(self._metalake.register_job_template(template))

        # Retrieve the registered job template
        retrieved_template = self._metalake.get_job_template(template.name)
        self.assertEqual(template, retrieved_template)

        # Test retrieving a non-existent job template
        with self.assertRaises(NoSuchJobTemplateException):
            self._metalake.get_job_template("non_existent_template")

    def test_register_and_delete_job_template(self):
        template1 = self.builder.with_name("test_1").build()
        template2 = self.builder.with_name("test_2").build()

        # Assert no exceptions are raised when registering job templates
        self.assertIsNone(self._metalake.register_job_template(template1))
        self.assertIsNone(self._metalake.register_job_template(template2))

        # List registered job templates
        registered_templates = self._metalake.list_job_templates()
        self.assertEqual(len(registered_templates), 2)
        self.assertIn(template1, registered_templates)
        self.assertIn(template2, registered_templates)

        # Retrieve and verify the registered job templates
        result1 = self._metalake.get_job_template(template1.name)
        result2 = self._metalake.get_job_template(template2.name)
        self.assertEqual(template1, result1)
        self.assertEqual(template2, result2)

        # Delete the first job template
        self.assertTrue(self._metalake.delete_job_template(template1.name))

        # Verify the first job template is deleted
        with self.assertRaises(NoSuchJobTemplateException):
            self._metalake.get_job_template(template1.name)

        # Verify the second job template still exists
        remaining_template = self._metalake.get_job_template(template2.name)
        self.assertEqual(template2, remaining_template)

        # Verify the list of job templates after deletion
        registered_templates = self._metalake.list_job_templates()
        self.assertEqual(len(registered_templates), 1)
        self.assertIn(template2, registered_templates)

        # Test deleting a non-existent job template
        self.assertFalse(self._metalake.delete_job_template(template1.name))

        # Delete the second job template
        self.assertTrue(self._metalake.delete_job_template(template2.name))

        # Verify the second job template is deleted
        with self.assertRaises(NoSuchJobTemplateException):
            self._metalake.get_job_template(template2.name)

        # Verify the list of job templates is empty after deleting both
        registered_templates = self._metalake.list_job_templates()
        self.assertTrue(len(registered_templates) == 0)

    def test_run_and_list_jobs(self):
        template = self.builder.with_name("test_run").build()
        self._metalake.register_job_template(template)

        # Submit jobs
        job_handle1 = self._metalake.run_job(
            template.name, {"arg1": "value1", "arg2": "success", "env_var": "value2"}
        )
        self.assertEqual(job_handle1.job_status(), JobHandle.Status.QUEUED)
        self.assertEqual(job_handle1.job_template_name(), template.name)

        job_handle2 = self._metalake.run_job(
            template.name, {"arg1": "value3", "arg2": "success", "env_var": "value4"}
        )
        self.assertEqual(job_handle2.job_status(), JobHandle.Status.QUEUED)
        self.assertEqual(job_handle2.job_template_name(), template.name)

        # List jobs
        jobs = self._metalake.list_jobs(template.name)
        self.assertEqual(len(jobs), 2)
        job_ids = [job.job_id() for job in jobs]
        self.assertIn(job_handle1.job_id(), job_ids)
        self.assertIn(job_handle2.job_id(), job_ids)

        # Wait for jobs to complete
        self._wait_until(
            lambda: self._metalake.get_job(job_handle1.job_id()).job_status()
            == JobHandle.Status.SUCCEEDED,
            timeout=180,
        )

        self._wait_until(
            lambda: self._metalake.get_job(job_handle2.job_id()).job_status()
            == JobHandle.Status.SUCCEEDED,
            timeout=180,
        )

        updated_jobs = self._metalake.list_jobs(template.name)
        self.assertEqual(len(updated_jobs), 2)
        job_statuses = {job.job_status() for job in updated_jobs}
        self.assertEqual(len(job_statuses), 1)
        self.assertIn(JobHandle.Status.SUCCEEDED, job_statuses)

    def test_run_and_get_job(self):
        template = self.builder.with_name("test_run_get").build()
        self._metalake.register_job_template(template)

        # Submit a job
        job_handle = self._metalake.run_job(
            template.name, {"arg1": "value1", "arg2": "success", "env_var": "value2"}
        )
        self.assertEqual(job_handle.job_status(), JobHandle.Status.QUEUED)
        self.assertEqual(job_handle.job_template_name(), template.name)

        # Wait for job to complete
        self._wait_until(
            lambda: self._metalake.get_job(job_handle.job_id()).job_status()
            == JobHandle.Status.SUCCEEDED,
            timeout=180,
        )
        retrieved_job = self._metalake.get_job(job_handle.job_id())
        self.assertEqual(job_handle.job_id(), retrieved_job.job_id())
        self.assertEqual(JobHandle.Status.SUCCEEDED, retrieved_job.job_status())

        # Test failed job
        failed_job_handle = self._metalake.run_job(
            template.name, {"arg1": "value1", "arg2": "fail", "env_var": "value2"}
        )
        self.assertEqual(failed_job_handle.job_status(), JobHandle.Status.QUEUED)

        self._wait_until(
            lambda: self._metalake.get_job(failed_job_handle.job_id()).job_status()
            == JobHandle.Status.FAILED,
            timeout=180,
        )
        retrieved_failed_job = self._metalake.get_job(failed_job_handle.job_id())
        self.assertEqual(failed_job_handle.job_id(), retrieved_failed_job.job_id())
        self.assertEqual(JobHandle.Status.FAILED, retrieved_failed_job.job_status())

        # Test non-existent job
        with self.assertRaises(NoSuchJobException):
            self._metalake.get_job("non_existent_job_id")

    def test_run_and_cancel_job(self):
        template = self.builder.with_name("test_run_cancel").build()
        self._metalake.register_job_template(template)

        # Submit a job
        job_handle = self._metalake.run_job(
            template.name, {"arg1": "value1", "arg2": "success", "env_var": "value2"}
        )
        self.assertEqual(job_handle.job_status(), JobHandle.Status.QUEUED)
        self.assertEqual(job_handle.job_template_name(), template.name)

        sleep(1)

        # Cancel the job
        self._metalake.cancel_job(job_handle.job_id())

        self._wait_until(
            lambda: self._metalake.get_job(job_handle.job_id()).job_status()
            == JobHandle.Status.CANCELLED,
            timeout=180,
        )

        retrieved_job = self._metalake.get_job(job_handle.job_id())
        self.assertEqual(job_handle.job_id(), retrieved_job.job_id())
        self.assertEqual(JobHandle.Status.CANCELLED, retrieved_job.job_status())

        # Test cancel non-existent job
        with self.assertRaises(NoSuchJobException):
            self._metalake.cancel_job("non_existent_job_id")

    def _wait_until(self, condition, timeout=180, interval=0.5):
        end_time = time.time() + timeout
        while time.time() < end_time:
            if condition():
                return True
            time.sleep(interval)
        raise TimeoutError(f"Condition not met within {timeout} seconds")

    @classmethod
    def generate_test_entry_script(cls):
        content = """#!/bin/bash
echo "starting test test job"

bin="$(dirname "${BASH_SOURCE-$0}")"
bin="$(cd "${bin}">/dev/null; pwd)"

. "${bin}/common.sh"

sleep 3

JOB_NAME="test_job-$(date +%s)-$1"

echo "Submitting job with name: $JOB_NAME"

echo "$1"

echo "$2"

echo "$ENV_VAR"

if [[ "$2" == "success" ]]; then
  exit 0
elif [[ "$2" == "fail" ]]; then
  exit 1
else
  exit 2
fi
"""
        script_path = Path(cls.test_staging_dir) / "test-job.sh"
        script_path.write_text(content, encoding="utf-8")
        script_path.chmod(0o755)
        return str(script_path)

    @classmethod
    def generate_test_lib_script(cls):
        content = """#!/bin/bash
echo "in common script"
"""
        script_path = Path(cls.test_staging_dir) / "common.sh"
        script_path.write_text(content, encoding="utf-8")
        script_path.chmod(0o755)
        return str(script_path)
