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
from abc import abstractmethod, ABC
from typing import List, Dict, Optional, TypeVar, Generic, Any
from enum import Enum
from copy import deepcopy


class JobType(Enum):
    """
    JobType is an enum to define the type of the job.

    Gravitino supports different types of jobs, such as Spark and Shell. The job type is
    required to determine the runtime environment for executing the job.
    """

    SPARK = "spark"
    """job type for executing Spark jobs"""
    SHELL = "shell"
    """job type for executing shell commands"""

    @classmethod
    def job_type_serialize(cls, job_type: "JobType") -> str:
        """
        Serializes the JobType to a string.

        Args:
            job_type: The JobType to serialize.

        Returns:
            The serialized string representation of the JobType.
        """
        return job_type.value.lower()

    @classmethod
    def job_type_deserialize(cls, job_type_str: str) -> "JobType":
        """
        Deserializes a string to a JobType.

        Args:
            job_type_str: The string representation of the JobType.

        Returns:
            The JobType corresponding to the string.
        """
        for m in cls:
            if m.value.lower() == job_type_str.lower():
                return m

        # If no match found, raise an error
        raise ValueError(f"Invalid job type string: {job_type_str}")


T = TypeVar("T", bound="JobTemplate")
B = TypeVar("B", bound="BaseBuilder")


class JobTemplate(ABC):
    """
    JobTemplate is a class to define all the configuration parameters for a job.

    JobType is enum to define the type of the job, because Gravitino needs different runtime
    environments to execute different types of jobs, so the job type is required.

    Some parameters can be templated, which means that they can be replaced with actual values
    when running the job, for example, arguments can be { "{input_path}}", "{{output_path}" },
    environment variables can be { "foo": "{{foo_value}}", "bar": "{{bar_value}}" }. the parameters
    support templating are: arguments, environments.
    """

    def __init__(self, builder: "BaseBuilder"):
        self._name = builder.name
        self._comment = builder.comment
        self._executable = builder.executable
        self._arguments = builder.arguments
        self._environments = builder.environments
        self._custom_fields = builder.custom_fields

    @property
    def name(self) -> str:
        """
        Returns:
            The name of the job template.
        """
        return self._name

    @property
    def comment(self) -> Optional[str]:
        """
        Returns:
            The comment of the job template, which can be used to describe the job.
            This field is optional and can be None.
        """
        return self._comment

    @property
    def executable(self) -> str:
        """
        Returns:
            The executable of the job template, which is the command or script that will be executed
            when the job is run. This field is required and must not be empty.
        """
        return self._executable

    @property
    def arguments(self) -> List[str]:
        """
        Returns:
            The arguments of the job template, which are the parameters that will be passed to the
            executable when the job is run.
        """
        return self._arguments

    @property
    def environments(self) -> Dict[str, str]:
        """
        Returns:
            The environment variables of the job template, which are key-value pairs that will be
            set in the environment when the job is run. This can include variables that can be
            templated with actual values when running the job.
        """
        return self._environments

    @property
    def custom_fields(self) -> Dict[str, str]:
        """
        Returns:
            Custom fields of the job template, which are additional key-value pairs that can be
            used to store any other information related to the job. This field is optional
            and can be empty.
        """
        return self._custom_fields

    @abstractmethod
    def job_type(self) -> JobType:
        """
        Returns:
            The type of the job template, which is an enum value indicating the type of job
            (e.g., SPARK, SHELL). This is required to determine the runtime environment for executing
            the job.
        """
        pass

    def __eq__(self, other: Any) -> bool:
        if self is other:
            return True
        if not isinstance(other, JobTemplate):
            return False
        return (
            self.job_type() == other.job_type()
            and self._name == other.name
            and self._comment == other.comment
            and self._executable == other.executable
            and self._arguments == other.arguments
            and self._environments == other.environments
            and self._custom_fields == other.custom_fields
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.job_type(),
                self._name,
                self._comment,
                self._executable,
                tuple(self._arguments),
                frozenset(self._environments.items()),
                frozenset(self._custom_fields.items()),
            )
        )

    def __str__(self) -> str:
        res = f"  jobType={self.job_type()},\n  name='{self._name}',\n"
        if self._comment:
            res += f"  comment='{self._comment}',\n"
        res += f"  executable='{self._executable}',\n"

        if self._arguments:
            res += (
                "  arguments=[\n"
                + "".join(f"    {arg},\n" for arg in self._arguments)
                + "  ],\n"
            )
        else:
            res += "  arguments=[],\n"

        if self._environments:
            res += (
                "  environments={\n"
                + "".join(f"    {k}: {v},\n" for k, v in self._environments.items())
                + "  },\n"
            )
        else:
            res += "  environments={},\n"

        if self._custom_fields:
            res += (
                "  customFields={\n"
                + "".join(f"    {k}: {v},\n" for k, v in self._custom_fields.items())
                + "  }\n"
            )
        else:
            res += "  customFields={}\n"

        return res

    class BaseBuilder(Generic[B, T]):
        """
        BaseBuilder is a generic class to build a JobTemplate.
        """

        def __init__(self: B):
            self.name: Optional[str] = None
            self.comment: Optional[str] = None
            self.executable: Optional[str] = None
            self.arguments: List[str] = []
            self.environments: Dict[str, str] = {}
            self.custom_fields: Dict[str, str] = {}

        def with_name(self: B, name: str) -> B:
            """
            Sets the name of the job template.

            Args:
                name: The name of the job template. It must not be null or empty.

            Returns:
                The builder instance for method chaining.
            """
            self.name = name
            return self

        def with_comment(self: B, comment: str) -> B:
            """
            Sets the comment of the job template.

            Args:
                comment: The comment of the job template. It can be null or empty.

            Returns:
                The builder instance for method chaining.
            """
            self.comment = comment
            return self

        def with_executable(self: B, executable: str) -> B:
            """
            Sets the executable of the job template.

            Args:
                executable: The executable of the job template. It must not be null or empty.

            Returns:
                The builder instance for method chaining.
            """
            self.executable = executable
            return self

        def with_arguments(self: B, arguments: List[str]) -> B:
            """
            Sets the arguments of the job template.

            Args:
                arguments: The arguments of the job template.

            Returns:
                The builder instance for method chaining.
            """
            self.arguments = deepcopy(arguments)
            return self

        def with_environments(self: B, environments: Dict[str, str]) -> B:
            """
            Sets the environment variables of the job template.

            Args:
                environments: The environment variables of the job template.

            Returns:
                The builder instance for method chaining.
            """
            self.environments = deepcopy(environments)
            return self

        def with_custom_fields(self: B, custom_fields: Dict[str, str]) -> B:
            """
            Sets the custom fields of the job template.

            Args:
                custom_fields: The custom fields of the job template.

            Returns:
                The builder instance for method chaining.
            """
            self.custom_fields = deepcopy(custom_fields)
            return self

        def validate(self: B):
            """
            Validates the job template parameters.
            """
            if not self.name or not self.name.strip():
                raise ValueError("Job name must not be null or empty")
            if not self.executable or not self.executable.strip():
                raise ValueError("Executable must not be null or empty")
            self.arguments = self.arguments or []
            self.environments = self.environments or {}
            self.custom_fields = self.custom_fields or {}

        @abstractmethod
        def build(self) -> T:
            """
            Builds the JobTemplate instance.

            Returns:
                An instance of JobTemplate with the configured parameters.
            """
            pass
