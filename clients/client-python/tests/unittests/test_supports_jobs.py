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
import unittest
from http.client import HTTPResponse
from unittest.mock import Mock, patch

from gravitino import GravitinoClient
from gravitino.api.job.job_handle import JobHandle
from gravitino.api.job.job_template import JobType
from gravitino.api.job.shell_job_template import ShellJobTemplate
from gravitino.api.job.spark_job_template import SparkJobTemplate
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.job.job_dto import JobDTO
from gravitino.dto.job.shell_job_template_dto import ShellJobTemplateDTO
from gravitino.dto.job.spark_job_template_dto import SparkJobTemplateDTO
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.job_list_response import JobListResponse
from gravitino.dto.responses.job_response import JobResponse
from gravitino.dto.responses.job_template_list_response import JobTemplateListResponse
from gravitino.dto.responses.job_template_response import JobTemplateResponse
from gravitino.utils import Response
from tests.unittests import mock_base


@mock_base.mock_data
class TestSupportsJobs(unittest.TestCase):
    _metalake_name = "metalake_demo"

    def test_list_job_templates(self, *mock_methods):

        gravitino_client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
        )

        templates = [self._new_shell_job_template(), self._new_spark_job_template()]
        template_dtos = [
            self._new_shell_job_template_dto(templates[0]),
            self._new_spark_job_template_dto(templates[1]),
        ]
        job_template_list_resp = JobTemplateListResponse(
            _job_templates=template_dtos, _code=0
        )
        json_str = job_template_list_resp.to_json()
        mock_resp = self._mock_http_response(json_str)

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ):
            result_templates = gravitino_client.list_job_templates()
            self.assertEqual(templates, result_templates)

        # test with empty response
        job_template_list_resp = JobTemplateListResponse(_job_templates=[], _code=0)
        json_str = job_template_list_resp.to_json()
        mock_resp = self._mock_http_response(json_str)

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ):
            result_templates = gravitino_client.list_job_templates()
            self.assertEqual([], result_templates)

    def test_register_job_template(self, *mock_methods):
        gravitino_client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
        )

        shell_template = self._new_shell_job_template()
        resp = BaseResponse(_code=0)
        mock_resp = self._mock_http_response(resp.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.post", return_value=mock_resp
        ):
            gravitino_client.register_job_template(shell_template)

    def test_get_job_template(self, *mock_methods):
        gravitino_client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
        )

        shell_template = self._new_shell_job_template()
        shell_template_dto = self._new_shell_job_template_dto(shell_template)
        resp = JobTemplateResponse(_job_template=shell_template_dto, _code=0)
        mock_resp = self._mock_http_response(resp.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ):
            result_template = gravitino_client.get_job_template(shell_template.name)
            self.assertEqual(shell_template, result_template)

    def test_delete_job_template(self, *mock_methods):
        gravitino_client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
        )

        shell_template = self._new_shell_job_template()
        resp = DropResponse(_dropped=True, _code=0)
        mock_resp = self._mock_http_response(resp.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete", return_value=mock_resp
        ):
            result = gravitino_client.delete_job_template(shell_template.name)
            self.assertTrue(result)

        resp = DropResponse(_dropped=False, _code=0)
        mock_resp = self._mock_http_response(resp.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete", return_value=mock_resp
        ):
            result = gravitino_client.delete_job_template(shell_template.name)
            self.assertFalse(result)

    def test_list_jobs(self, *mock_methods):
        gravitino_client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
        )

        job_template_name = "test_shell_job"
        job_dtos = [
            self._new_job_dto(job_template_name),
            self._new_job_dto(job_template_name),
        ]
        resp = JobListResponse(_jobs=job_dtos, _code=0)
        mock_resp = self._mock_http_response(resp.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ):
            result_jobs = gravitino_client.list_jobs(job_template_name)
            self.assertEqual(len(job_dtos), len(result_jobs))
            for i, job_dto in enumerate(job_dtos):
                self._compare_job_handle(result_jobs[i], job_dto)

    def test_run_job(self, *mock_methods):
        gravitino_client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
        )

        job_template_name = "test_shell_job"
        job_dto = self._new_job_dto(job_template_name)
        resp = JobResponse(_job=job_dto, _code=0)
        mock_resp = self._mock_http_response(resp.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.post", return_value=mock_resp
        ):
            job_handle = gravitino_client.run_job(job_template_name, {})
            self._compare_job_handle(job_handle, job_dto)

    def test_get_job(self, *mock_methods):
        gravitino_client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
        )

        job_template_name = "test_shell_job"
        job_dto = self._new_job_dto(job_template_name)
        resp = JobResponse(_job=job_dto, _code=0)
        mock_resp = self._mock_http_response(resp.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ):
            job_handle = gravitino_client.get_job(job_dto.job_id())
            self._compare_job_handle(job_handle, job_dto)

        # test with invalid input
        with self.assertRaises(ValueError):
            gravitino_client.get_job("")

        with self.assertRaises(ValueError):
            gravitino_client.get_job(None)

    def test_cancel_job(self, *mock_methods):
        gravitino_client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
        )

        job_template_name = "test_shell_job"
        job_dto = self._new_job_dto(job_template_name)
        resp = JobResponse(_job=job_dto, _code=0)
        mock_resp = self._mock_http_response(resp.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.post", return_value=mock_resp
        ):
            job_handle = gravitino_client.cancel_job(job_dto.job_id())
            self._compare_job_handle(job_handle, job_dto)

        # test with invalid input
        with self.assertRaises(ValueError):
            gravitino_client.cancel_job("")

        with self.assertRaises(ValueError):
            gravitino_client.cancel_job(None)

    def _new_shell_job_template(self):
        return (
            ShellJobTemplate.builder()
            .with_name("test_shell_job")
            .with_comment("This is a test shell job template")
            .with_executable("/path/to/executable.sh")
            .with_arguments(["arg1", "arg2"])
            .with_environments({"ENV_VAR": "value"})
            .with_custom_fields({"custom_field": "custom_value"})
            .with_scripts(["/path/to/script1.sh", "/path/to/script2.sh"])
            .build()
        )

    def _new_spark_job_template(self):
        return (
            SparkJobTemplate.builder()
            .with_name("test_spark_job")
            .with_comment("This is a test spark job template")
            .with_executable("/path/to/spark-demo.jar")
            .with_class_name("com.example.SparkJob")
            .with_jars(["/path/to/jar1.jar", "/path/to/jar2.jar"])
            .with_files(["/path/to/file1.txt", "/path/to/file2.txt"])
            .with_archives(["/path/to/archive1.zip", "/path/to/archive2.zip"])
            .with_configs({"spark.executor.memory": "2g", "spark.driver.memory": "1g"})
            .build()
        )

    def _new_shell_job_template_dto(self, template: ShellJobTemplate):
        return ShellJobTemplateDTO(
            _job_type=JobType.SHELL,
            _name=template.name,
            _comment=template.comment,
            _executable=template.executable,
            _arguments=template.arguments,
            _environments=template.environments,
            _custom_fields=template.custom_fields,
            _scripts=template.scripts,
        )

    def _new_spark_job_template_dto(self, template: SparkJobTemplate):
        return SparkJobTemplateDTO(
            _job_type=JobType.SPARK,
            _name=template.name,
            _comment=template.comment,
            _executable=template.executable,
            _arguments=template.arguments,
            _environments=template.environments,
            _custom_fields=template.custom_fields,
            _class_name=template.class_name,
            _jars=template.jars,
            _files=template.files,
            _archives=template.archives,
            _configs=template.configs,
        )

    def _mock_http_response(self, json_str: str):
        mock_http_resp = Mock(HTTPResponse)
        mock_http_resp.getcode.return_value = 200
        mock_http_resp.read.return_value = json_str
        mock_http_resp.info.return_value = None
        mock_http_resp.url = None
        mock_resp = Response(mock_http_resp)
        return mock_resp

    def _new_job_dto(self, job_template_name: str) -> JobDTO:
        return JobDTO(
            _job_id="job-123",
            _job_template_name=job_template_name,
            _status=JobHandle.Status.QUEUED,
            _audit=AuditDTO(_creator="test", _create_time="2023-10-01T00:00:00Z"),
        )

    def _compare_job_handle(self, job_handle: JobHandle, job_dto: JobDTO):
        self.assertEqual(job_handle.job_id(), job_dto.job_id())
        self.assertEqual(job_handle.job_template_name(), job_dto.job_template_name())
        self.assertEqual(job_handle.job_status(), job_dto.status())
