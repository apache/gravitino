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
from gravitino.api.job.job_template_change import (
    JobTemplateChange,
    ShellTemplateUpdate,
    SparkTemplateUpdate,
)
from gravitino.api.job.shell_job_template import ShellJobTemplate
from gravitino.client.dto_converters import DTOConverters
from gravitino.exceptions.base import (
    JobTemplateAlreadyExistsException,
    NoSuchJobTemplateException,
    NoSuchJobException,
    IllegalArgumentException,
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
        self.assertIn(template2, registered_templates)

        # Test deleting a non-existent job template
        self.assertFalse(self._metalake.delete_job_template(template1.name))

        # Delete the second job template
        self.assertTrue(self._metalake.delete_job_template(template2.name))

        # Verify the second job template is deleted
        with self.assertRaises(NoSuchJobTemplateException):
            self._metalake.get_job_template(template2.name)

    def test_register_and_alter_job_template(self):
        template = self.builder.with_name("test_alter").build()
        self._metalake.register_job_template(template)

        # Rename the job template
        updated_template = self._metalake.alter_job_template(
            template.name, JobTemplateChange.rename("test_alter_renamed")
        )
        self.assertEqual("test_alter_renamed", updated_template.name)
        self.assertEqual(template.executable, updated_template.executable)
        self.assertEqual(template.arguments, updated_template.arguments)
        self.assertEqual(template.environments, updated_template.environments)
        self.assertEqual(template.custom_fields, updated_template.custom_fields)
        self.assertEqual(
            template.scripts,
            DTOConverters.to_shell_job_template(updated_template).scripts,
        )

        fetched_template = self._metalake.get_job_template("test_alter_renamed")
        self.assertEqual(updated_template, fetched_template)

        # Update the comment of the job template
        updated_template = self._metalake.alter_job_template(
            "test_alter_renamed", JobTemplateChange.update_comment("Updated comment")
        )
        self.assertEqual("Updated comment", updated_template.comment)
        self.assertEqual("test_alter_renamed", updated_template.name)
        self.assertEqual(template.executable, updated_template.executable)
        self.assertEqual(template.arguments, updated_template.arguments)
        self.assertEqual(template.environments, updated_template.environments)
        self.assertEqual(template.custom_fields, updated_template.custom_fields)
        self.assertEqual(
            template.scripts,
            DTOConverters.to_shell_job_template(updated_template).scripts,
        )

        fetched_template = self._metalake.get_job_template("test_alter_renamed")
        self.assertEqual(updated_template, fetched_template)

        # Update the content of the job template
        updated_template_content = ShellTemplateUpdate(
            new_executable="/bin/echo",
            new_arguments=["Hello", "World"],
            new_environments={"NEW_ENV": "new_value"},
        )
        updated_template = self._metalake.alter_job_template(
            "test_alter_renamed",
            JobTemplateChange.update_template(updated_template_content),
        )

        self.assertEqual("/bin/echo", updated_template.executable)
        self.assertEqual(["Hello", "World"], updated_template.arguments)
        self.assertEqual({"NEW_ENV": "new_value"}, updated_template.environments)
        self.assertEqual("Updated comment", updated_template.comment)
        self.assertEqual("test_alter_renamed", updated_template.name)
        self.assertEqual(template.custom_fields, updated_template.custom_fields)
        self.assertEqual(
            template.scripts,
            DTOConverters.to_shell_job_template(updated_template).scripts,
        )

        fetched_template = self._metalake.get_job_template("test_alter_renamed")
        self.assertEqual(updated_template, fetched_template)

        # Update the custom fields and scripts of the job template
        updated_template_content = ShellTemplateUpdate(
            new_custom_fields={"key1": "value1", "key2": "value2"},
            new_scripts=["/bin/script1.sh", "/bin/script2.sh"],
        )
        updated_template = self._metalake.alter_job_template(
            "test_alter_renamed",
            JobTemplateChange.update_template(updated_template_content),
        )

        self.assertEqual("/bin/echo", updated_template.executable)
        self.assertEqual(["Hello", "World"], updated_template.arguments)
        self.assertEqual({"NEW_ENV": "new_value"}, updated_template.environments)
        self.assertEqual("Updated comment", updated_template.comment)
        self.assertEqual("test_alter_renamed", updated_template.name)
        self.assertEqual(
            {"key1": "value1", "key2": "value2"}, updated_template.custom_fields
        )
        self.assertEqual(
            ["/bin/script1.sh", "/bin/script2.sh"],
            DTOConverters.to_shell_job_template(updated_template).scripts,
        )

        fetched_template = self._metalake.get_job_template("test_alter_renamed")
        self.assertEqual(updated_template, fetched_template)

        # Test altering a non-existent job template
        with self.assertRaises(NoSuchJobTemplateException):
            self._metalake.alter_job_template(
                "non_existent", JobTemplateChange.rename("should_fail")
            )

        # Test altering with the wrong type of change
        with self.assertRaises(IllegalArgumentException):
            wrong_update = SparkTemplateUpdate(new_executable="/bin/echo")
            self._metalake.alter_job_template(
                "test_alter_renamed", JobTemplateChange.update_template(wrong_update)
            )

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

    def test_job_template_with_optional_arguments(self):
        opt_arg_test_script_path = self.generate_optional_arg_test_script()

        # Template with flag/value optional pairs. {{mode}} tells the validation
        # script which flags it should or should not receive.
        template = (
            ShellJobTemplate.builder()
            .with_name("test_optional_args_template")
            .with_comment("Test template with optional flag/value pairs")
            .with_executable(opt_arg_test_script_path)
            .with_arguments(
                [
                    "{{mode}}",
                    "?--opt1",
                    "?{{optional_arg1}}",
                    "?--opt2",
                    "?{{optional_arg2}}",
                ]
            )
            .with_environments({})
            .with_scripts([])
            .with_custom_fields({})
            .build()
        )

        self._metalake.register_job_template(template)
        self.assertIsNotNone(self._metalake.get_job_template(template.name))

        # Case 1: No optional args — script asserts neither flag is received.
        self._run_job_and_await(
            template, {"mode": "no_optionals"}, JobHandle.Status.SUCCEEDED
        )

        # Case 2: opt1 provided, opt2 absent — script asserts --opt1 present, --opt2 absent.
        self._run_job_and_await(
            template,
            {"mode": "opt1_only", "optional_arg1": "val1"},
            JobHandle.Status.SUCCEEDED,
        )

        # Case 3: Both optional args — script asserts both flags are received.
        self._run_job_and_await(
            template,
            {
                "mode": "both_optionals",
                "optional_arg1": "val1",
                "optional_arg2": "val2",
            },
            JobHandle.Status.SUCCEEDED,
        )

    def _run_job_and_await(self, template, job_conf, expected_status):
        job = self._metalake.run_job(template.name, job_conf)
        self.assertIsNotNone(job)
        self._wait_until(
            lambda: self._metalake.get_job(job.job_id()).job_status()
            in (JobHandle.Status.SUCCEEDED, JobHandle.Status.FAILED),
            timeout=30,
        )
        self.assertEqual(
            expected_status, self._metalake.get_job(job.job_id()).job_status()
        )

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

    # $1 = mode; remaining args are parsed for --opt1 and --opt2 flags.
    # Exits 0 when the received flags match the state encoded in mode, 1 otherwise.
    @classmethod
    def generate_optional_arg_test_script(cls):
        content = """#!/bin/bash
MODE="$1"
shift
OPT1_PRESENT=0
OPT2_PRESENT=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --opt1) OPT1_PRESENT=1; shift 2;;
    --opt2) OPT2_PRESENT=1; shift 2;;
    *) shift;;
  esac
done
case "$MODE" in
  no_optionals)
    [[ $OPT1_PRESENT -eq 0 && $OPT2_PRESENT -eq 0 ]] && exit 0 || exit 1;;
  opt1_only)
    [[ $OPT1_PRESENT -eq 1 && $OPT2_PRESENT -eq 0 ]] && exit 0 || exit 1;;
  both_optionals)
    [[ $OPT1_PRESENT -eq 1 && $OPT2_PRESENT -eq 1 ]] && exit 0 || exit 1;;
  *) exit 1;;
esac
"""
        script_path = Path(cls.test_staging_dir) / "optional-arg-test.sh"
        script_path.write_text(content, encoding="utf-8")
        script_path.chmod(0o755)
        return str(script_path)
