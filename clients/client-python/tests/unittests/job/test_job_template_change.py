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
from typing import cast

from gravitino.api.job.job_template_change import (
    JobTemplateChange,
    RenameJobTemplate,
    UpdateJobTemplateComment,
    UpdateJobTemplate,
    ShellTemplateUpdate,
    SparkTemplateUpdate,
)


class TestJobTemplateChange(unittest.TestCase):
    def test_rename_job_template(self):
        change = JobTemplateChange.rename("new_name")
        self.assertIsInstance(change, RenameJobTemplate)
        self.assertEqual(change.get_new_name(), "new_name")
        self.assertEqual(str(change), "RENAME JOB TEMPLATE new_name")

    def test_update_comment(self):
        change = JobTemplateChange.update_comment("new comment")
        self.assertIsInstance(change, UpdateJobTemplateComment)
        self.assertEqual(change.get_new_comment(), "new comment")
        self.assertEqual(str(change), "UPDATE JOB TEMPLATE COMMENT new comment")

    def test_update_shell_template(self):
        template_update = ShellTemplateUpdate(
            new_executable="/bin/bash",
            new_arguments=["-c", "echo hello"],
            new_environments={"ENV": "test"},
            new_custom_fields={"field": "value"},
            new_scripts=["script1", "echo script1 content"],
        )
        change = JobTemplateChange.update_template(template_update)
        self.assertIsInstance(change, UpdateJobTemplate)
        self.assertEqual(change.get_template_update().get_new_executable(), "/bin/bash")
        self.assertEqual(
            change.get_template_update().get_new_arguments(), ["-c", "echo hello"]
        )
        self.assertEqual(
            change.get_template_update().get_new_environments(), {"ENV": "test"}
        )
        self.assertEqual(
            change.get_template_update().get_new_custom_fields(), {"field": "value"}
        )
        self.assertEqual(
            cast(ShellTemplateUpdate, change.get_template_update()).get_new_scripts(),
            ["script1", "echo script1 content"],
        )

    def test_update_spark_template(self):

        template_update = SparkTemplateUpdate(
            new_executable="spark-submit",
            new_arguments=["--class", "org.example.Main", "app.jar"],
            new_environments={"SPARK_ENV": "prod"},
            new_custom_fields={"spark_field": "spark_value"},
            new_class_name="org.example.Main",
            new_jars=["lib1.jar", "lib2.jar"],
            new_files=["file1.txt", "file2.txt"],
            new_archives=["archive1.zip"],
            new_configs={"spark.executor.memory": "4g"},
        )
        change = JobTemplateChange.update_template(template_update)
        self.assertIsInstance(change, UpdateJobTemplate)
        self.assertEqual(
            change.get_template_update().get_new_executable(), "spark-submit"
        )
        self.assertEqual(
            change.get_template_update().get_new_arguments(),
            ["--class", "org.example.Main", "app.jar"],
        )
        self.assertEqual(
            change.get_template_update().get_new_environments(), {"SPARK_ENV": "prod"}
        )
        self.assertEqual(
            change.get_template_update().get_new_custom_fields(),
            {"spark_field": "spark_value"},
        )
        spark_update = cast(SparkTemplateUpdate, change.get_template_update())
        self.assertEqual(spark_update.get_new_class_name(), "org.example.Main")
        self.assertEqual(spark_update.get_new_jars(), ["lib1.jar", "lib2.jar"])
        self.assertEqual(spark_update.get_new_files(), ["file1.txt", "file2.txt"])
        self.assertEqual(spark_update.get_new_archives(), ["archive1.zip"])
        self.assertEqual(
            spark_update.get_new_configs(), {"spark.executor.memory": "4g"}
        )
