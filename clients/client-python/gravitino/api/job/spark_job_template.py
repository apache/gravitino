# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

from copy import deepcopy
from typing import List, Dict, Optional, Any
from .job_template import JobTemplate, JobType


class SparkJobTemplate(JobTemplate):
    """
    Represents a job template for executing Spark jobs.
    """

    def __init__(self, builder: "SparkJobTemplate.Builder"):
        """
        Initializes a SparkJobTemplate instance.
        """
        super().__init__(builder)
        self._class_name = builder.class_name
        self._jars = builder.jars
        self._files = builder.files
        self._archives = builder.archives
        self._configs = builder.configs

    @property
    def class_name(self) -> str:
        """
        Returns the class name of the Spark job.

        Returns:
            the class name as a string.
        """
        return self._class_name

    @property
    def jars(self) -> List[str]:
        """
        Returns the list of JAR files associated with this Spark job template.

        Returns:
            List of JAR file paths.
        """
        return self._jars

    @property
    def files(self) -> List[str]:
        """
        Returns the list of files associated with this Spark job template.

        Returns:
            List of file paths.
        """
        return self._files

    @property
    def archives(self) -> List[str]:
        """
        Returns the list of archives associated with this Spark job template.

        Returns:
            List of archive file paths.
        """
        return self._archives

    @property
    def configs(self) -> Dict[str, str]:
        """
        Returns the configuration settings for this Spark job template.

        Returns:
            A dictionary of configuration key-value pairs.
        """
        return self._configs

    def job_type(self) -> JobType:
        """
        Returns the type of the job, which is SPARK for this template.

        Returns:
            JobType.SPARK: Indicates that this is a Spark job template.
        """
        return JobType.SPARK

    @staticmethod
    def builder() -> "SparkJobTemplate.Builder":
        """
        Creates a new builder instance for constructing a SparkJobTemplate.

        Returns:
            SparkJobTemplate.Builder: A new builder instance.
        """
        return SparkJobTemplate.Builder()

    def __eq__(self, other: Any) -> bool:
        if not super().__eq__(other):
            return False
        return (
            self._class_name == other.class_name
            and self._jars == other.jars
            and self._files == other.files
            and self._archives == other.archives
            and self._configs == other.configs
        )

    def __hash__(self) -> int:
        return hash(
            (
                super().__hash__(),
                self._class_name,
                tuple(self._jars),
                tuple(self._files),
                tuple(self._archives),
                frozenset(self._configs.items()),
            )
        )

    def __str__(self) -> str:
        result = "\nSparkJobTemplate{\n"
        result += f"  className='{self.class_name}',\n"

        if self._jars:
            result += (
                "  jars=[\n" + "".join(f"    {j},\n" for j in self._jars) + "  ],\n"
            )
        else:
            result += "  jars=[],\n"

        if self._files:
            result += (
                "  files=[\n" + "".join(f"    {f},\n" for f in self._files) + "  ],\n"
            )
        else:
            result += "  files=[],\n"

        if self._archives:
            result += (
                "  archives=[\n"
                + "".join(f"    {a},\n" for a in self._archives)
                + "  ],\n"
            )
        else:
            result += "  archives=[],\n"

        if self._configs:
            result += (
                "  configs={\n"
                + "".join(f"    {k}: {v},\n" for k, v in self.configs.items())
                + "  },\n"
            )
        else:
            result += "  configs={},\n"

        return result + super().__str__() + "}\n"

    class Builder(
        JobTemplate.BaseBuilder["SparkJobTemplate.Builder", "SparkJobTemplate"]
    ):
        """
        Builder class for creating SparkJobTemplate instances.
        """

        def __init__(self):
            """
            Initializes the builder with default values.
            """
            super().__init__()
            self.class_name: Optional[str] = None
            self.jars: List[str] = []
            self.files: List[str] = []
            self.archives: List[str] = []
            self.configs: Dict[str, str] = {}

        def with_class_name(self, class_name: str) -> "SparkJobTemplate.Builder":
            """
            Sets the class name for this Spark job template.

            Args:
                class_name: The fully qualified name of the Spark job class.

            Returns:
                The builder instance for method chaining.
            """
            self.class_name = class_name
            return self

        def with_jars(self, jars: List[str]) -> "SparkJobTemplate.Builder":
            """
            Sets the JAR files for this Spark job template.

            Args:
                jars: A list of JAR file paths to be included in the Spark job.

            Returns:
                The builder instance for method chaining.
            """
            self.jars = deepcopy(jars)
            return self

        def with_files(self, files: List[str]) -> "SparkJobTemplate.Builder":
            """
            Sets the files for this Spark job template.

            Args:
                files: A list of file paths to be included in the Spark job.

            Returns:
                The builder instance for method chaining.
            """
            self.files = deepcopy(files)
            return self

        def with_archives(self, archives: List[str]) -> "SparkJobTemplate.Builder":
            """
            Sets the archives for this Spark job template.

            Args:
                archives: A list of archive file paths to be included in the Spark job.

            Returns:
                The builder instance for method chaining.
            """
            self.archives = deepcopy(archives)
            return self

        def with_configs(self, configs: Dict[str, str]) -> "SparkJobTemplate.Builder":
            """
            Sets the configuration settings for this Spark job template.

            Args:
                configs: A dictionary of configuration key-value pairs to be applied to the Spark job.

            Returns:
                The builder instance for method chaining.
            """
            self.configs = deepcopy(configs)
            return self

        def validate(self):
            """
            Validates the SparkJobTemplate properties.
            """
            super().validate()
            if not self.class_name or not self.class_name.strip():
                raise ValueError("Class name must not be null or empty")
            self.jars = self.jars or []
            self.files = self.files or []
            self.archives = self.archives or []
            self.configs = self.configs or {}

        def build(self) -> "SparkJobTemplate":
            """
            Builds and returns a SparkJobTemplate instance.
            """
            self.validate()
            return SparkJobTemplate(self)
