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
from typing import List, Any
from .job_template import JobTemplate, JobType


class ShellJobTemplate(JobTemplate):
    """
    Represents a job template for executing shell scripts.
    """

    def __init__(self, builder: "ShellJobTemplate.Builder"):
        """
        Initializes a ShellJobTemplate instance.
        """
        super().__init__(builder)
        self._scripts = builder.scripts

    @property
    def scripts(self) -> List[str]:
        """
        Returns the list of shell scripts associated with this job template.

        Returns:
            List of shell scripts.
        """
        return self._scripts

    def job_type(self) -> JobType:
        """
        Returns the type of the job, which is SHELL for this template.

        Returns:
            JobType.SHELL: Indicates that this is a shell job template.
        """
        return JobType.SHELL

    @staticmethod
    def builder() -> "ShellJobTemplate.Builder":
        """
        Creates a new builder instance for constructing a ShellJobTemplate.

        Returns:
            ShellJobTemplate.Builder: A new builder instance.
        """
        return ShellJobTemplate.Builder()

    def __eq__(self, other: Any) -> bool:
        if not super().__eq__(other):
            return False
        return self._scripts == other.scripts

    def __hash__(self) -> int:
        return hash((super().__hash__(), tuple(self._scripts)))

    def __str__(self) -> str:
        res = "\nSellJobTemplate{\n"
        if self._scripts:
            res += (
                "  scripts=[\n"
                + "".join(f"    {s},\n" for s in self._scripts)
                + "  ],\n"
            )
        else:
            res += "  scripts=[],\n"

        return res + super().__str__() + "}\n"

    class Builder(
        JobTemplate.BaseBuilder["ShellJobTemplate.Builder", "ShellJobTemplate"]
    ):
        """
        Builder class for creating ShellJobTemplate instances.
        """

        def __init__(self):
            """
            Initializes the builder with default values.
            """
            super().__init__()
            self.scripts: List[str] = []

        def with_scripts(self, scripts: List[str]) -> "ShellJobTemplate.Builder":
            """
            Sets the shell scripts for this job template.

            Args:
                scripts: A list of shell scripts to be executed by this job template.

            Returns:
                ShellJobTemplate.Builder: The builder instance for method chaining.
            """
            self.scripts = deepcopy(scripts)
            return self

        def validate(self):
            """
            Validates the builder state before building the ShellJobTemplate.
            """
            super().validate()
            self.scripts = self.scripts or []

        def build(self) -> "ShellJobTemplate":
            """
            Builds and returns a ShellJobTemplate instance.

            Returns:
                ShellJobTemplate: The constructed shell job template.
            """
            self.validate()
            return ShellJobTemplate(self)
