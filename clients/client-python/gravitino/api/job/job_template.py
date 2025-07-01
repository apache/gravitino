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

from dataclasses import dataclass, field
from typing import List, Dict, Optional
from enum import Enum


class JobType(Enum):
    """
    Job type enumeration.
    """

    SPARK = "SPARK"  # Job type for executing a Spark application.
    SHELL = "SHELL"  # Job type for executing a shell command.


@dataclass
class JobTemplate:
    """
    JobTemplate is a class to define all the configuration parameters for a job.
    <p>
    JobType is enum to define the type of the job, because Gravitino needs different runtime
    environments to execute different types of jobs, so the job type is required.
    <p>
    Some parameters can be templated, which means that they can be replaced with actual values when
    running the job, for example, arguments can be { "{input_path}}", "{{output_path}" },
    environment variables can be { "foo": "{{foo_value}}", "bar": "{{bar_value}}" }. the
    parameters support templating are: arguments, environments, configs
    <p>
    The artifacts (files, jars, archives) will be uploaded to the Gravitino server and managed in
    the staging path when registering the job.
    """

    # pylint: disable=too-many-instance-attributes

    job_type: Optional[JobType] = None
    name: Optional[str] = None
    comment: Optional[str] = None
    executable: Optional[str] = None
    arguments: List[str] = field(default_factory=list)
    configs: Dict[str, str] = field(default_factory=dict)
    environments: Dict[str, str] = field(default_factory=dict)
    files: List[str] = field(default_factory=list)
    jars: List[str] = field(default_factory=list)
    archives: List[str] = field(default_factory=list)

    @staticmethod
    def builder():
        """
        Create a new Builder instance for JobTemplate.
        """
        return JobTemplate.Builder()

    class Builder:
        """
        Builder class for constructing JobTemplate instances.
        """

        def __init__(self):
            self._job_template = JobTemplate()

        def with_job_type(self, job_type: JobType):
            """
            Set the job type.

            Args:
                job_type (JobType): The type of the job.

            Returns:
                Builder: This Builder instance.
            """
            self._job_template.job_type = job_type
            return self

        def with_name(self, name: str):
            """
            Set the name of the job.

            Args:
                name (str): The name of the job.

            Returns:
                Builder: This Builder instance.
            """
            self._job_template.name = name
            return self

        def with_comment(self, comment: str):
            """
            Set the comment for the job.

            Args:
                comment (str): The comment or description of the job.

            Returns:
                Builder: This Builder instance.
            """
            self._job_template.comment = comment
            return self

        def with_executable(self, executable: str):
            """
            Set the executable for the job.

            Args:
                executable (str): The path to the executable.

            Returns:
                Builder: This Builder instance.
            """
            self._job_template.executable = executable
            return self

        def with_arguments(self, arguments: List[str]):
            """
            Set the arguments for the job.

            Args:
                arguments (List[str]): The list of arguments.

            Returns:
                Builder: This Builder instance.
            """
            self._job_template.arguments = arguments
            return self

        def with_configs(self, configs: Dict[str, str]):
            """
            Set the configurations for the job.

            Args:
                configs (Dict[str, str]): The map of configurations.

            Returns:
                Builder: This Builder instance.
            """
            self._job_template.configs = configs
            return self

        def with_environments(self, environments: Dict[str, str]):
            """
            Set the environment variables for the job.

            Args:
                environments (Dict[str, str]): The map of environment variables.

            Returns:
                Builder: This Builder instance.
            """
            self._job_template.environments = environments
            return self

        def with_files(self, files: List[str]):
            """
            Set the files to be included in the job.

            Args:
                files (List[str]): The list of file paths.

            Returns:
                Builder: This Builder instance.
            """
            self._job_template.files = files
            return self

        def with_jars(self, jars: List[str]):
            """
            Set the jars to be included in the job.

            Args:
                jars (List[str]): The list of jar paths.

            Returns:
                Builder: This Builder instance.
            """
            self._job_template.jars = jars
            return self

        def with_archives(self, archives: List[str]):
            """
            Set the archives to be included in the job.

            Args:
                archives (List[str]): The list of archive paths.

            Returns:
                Builder: This Builder instance.
            """
            self._job_template.archives = archives
            return self

        def build(self):
            """
            Build the JobTemplate instance.

            Returns:
                JobTemplate: A new JobTemplate instance.

            Raises:
                ValueError: If required fields are missing or invalid.
            """
            if not self._job_template.job_type:
                raise ValueError("Job type must not be null")
            if not self._job_template.name or not self._job_template.name.strip():
                raise ValueError("Job name must not be null or empty")
            if (
                not self._job_template.executable
                or not self._job_template.executable.strip()
            ):
                raise ValueError("Executable must not be null or empty")

            # Ensure immutability for lists and dictionaries
            self._job_template.arguments = (
                []
                if self._job_template.arguments is None
                else self._job_template.arguments
            )
            self._job_template.configs = (
                {} if self._job_template.configs is None else self._job_template.configs
            )
            self._job_template.environments = (
                {}
                if self._job_template.environments is None
                else self._job_template.environments
            )
            self._job_template.files = (
                [] if self._job_template.files is None else self._job_template.files
            )
            self._job_template.jars = (
                [] if self._job_template.jars is None else self._job_template.jars
            )
            self._job_template.archives = (
                []
                if self._job_template.archives is None
                else self._job_template.archives
            )

            return self._job_template
