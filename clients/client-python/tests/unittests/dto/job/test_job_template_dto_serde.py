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

from gravitino.api.job.job_template import JobType
from gravitino.dto.job.job_template_dto import JobTemplateDTO
from gravitino.dto.job.shell_job_template_dto import ShellJobTemplateDTO
from gravitino.dto.job.spark_job_template_dto import SparkJobTemplateDTO


class TestJobTemplateDTOSerDe(unittest.TestCase):

    def test_shell_job_template_dto(self):
        shell_job = ShellJobTemplateDTO(
            _job_type=JobType.SHELL,
            _name="test_shell_job",
            _comment="Test shell job",
            _executable="/path/to/script.sh",
            _arguments=["arg1", "arg2"],
            _environments={"ENV_VAR": "value"},
            _custom_fields={"custom_field1": "value1"},
            _scripts=["/path/to/script1.sh", "/path/to/script2.sh"],
            _audit=None,
        )
        shell_job.validate()

        json_str = shell_job.to_json()
        shell_job_template_dto = JobTemplateDTO.from_json(json_str, infer_missing=True)
        self.assertIsInstance(shell_job_template_dto, ShellJobTemplateDTO)
        self.assertEqual(shell_job, shell_job_template_dto)

        shell_job = ShellJobTemplateDTO(
            _job_type=JobType.SHELL,
            _name="test_shell_job_1",
            _executable="/path/to/script1.sh",
            _audit=None,
        )
        shell_job.validate()

        json_str = shell_job.to_json()
        shell_job_template_dto = JobTemplateDTO.from_json(json_str, infer_missing=True)
        self.assertIsInstance(shell_job_template_dto, ShellJobTemplateDTO)
        self.assertEqual(shell_job, shell_job_template_dto)

    def test_spark_job_template_dto(self):
        spark_job = SparkJobTemplateDTO(
            _job_type=JobType.SPARK,
            _name="test_spark_job",
            _comment="Test spark job",
            _executable="/path/to/spark-demo.jar",
            _arguments=["input", "output"],
            _environments={"ENV_VAR": "value"},
            _custom_fields={"custom_field1": "value1"},
            _class_name="com.example.SparkDemo",
            _jars=["/path/to/jar1", "/path/to/jar2"],
            _files=["/path/to/file1", "/path/to/file2"],
            _archives=["/path/to/a.zip", "/path/to/b.zip"],
            _configs={"k1": "v1"},
            _audit=None,
        )
        spark_job.validate()

        json_str = spark_job.to_json()
        spark_job_template_dto = JobTemplateDTO.from_json(json_str, infer_missing=True)
        self.assertIsInstance(spark_job_template_dto, SparkJobTemplateDTO)
        self.assertEqual(spark_job, spark_job_template_dto)

        spark_job = SparkJobTemplateDTO(
            _job_type=JobType.SPARK,
            _name="test_spark_job",
            _comment="Test spark job",
            _executable="/path/to/spark-demo.jar",
            _class_name="com.example.SparkDemo",
            _audit=None,
        )
        spark_job.validate()

        json_str = spark_job.to_json()
        spark_job_template_dto = JobTemplateDTO.from_json(json_str, infer_missing=True)
        self.assertIsInstance(spark_job_template_dto, SparkJobTemplateDTO)
        self.assertEqual(spark_job, spark_job_template_dto)
