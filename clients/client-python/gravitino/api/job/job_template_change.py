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
from abc import ABC
from typing import List, Dict, Optional, Any
from copy import deepcopy


class JobTemplateChange:
    """
    The interface for job template changes. A job template change is an operation that modifies a job
    template. It can be one of the following:

      - Rename the job template.
      - Update the comment of the job template.
      - Update the job template details, such as executable, arguments, environments, custom
        fields, etc.
    """

    @staticmethod
    def rename(new_name: str) -> "JobTemplateChange":
        """
        Creates a new job template change to update the name of the job template.

        Args:
            new_name: The new name of the job template.

        Returns:
            The job template change.
        """
        return RenameJobTemplate(new_name)

    @staticmethod
    def update_comment(new_comment: str) -> "JobTemplateChange":
        """
        Creates a new job template change to update the comment of the job template.

        Args:
            new_comment: The new comment of the job template.

        Returns:
            The job template change.
        """
        return UpdateJobTemplateComment(new_comment)

    @staticmethod
    def update_template(template_update: "TemplateUpdate") -> "JobTemplateChange":
        """
        Creates a new job template change to update the details of the job template.

        Args:
            template_update: The template update details.

        Returns:
            The job template change.
        """
        return UpdateJobTemplate(template_update)


class RenameJobTemplate(JobTemplateChange):
    """
    A job template change to rename the job template.
    """

    def __init__(self, new_name: str):
        """
        Initialize a RenameJobTemplate change.

        Args:
            new_name: The new name of the job template.
        """
        self._new_name = new_name

    def get_new_name(self) -> str:
        """
        Get the new name of the job template.

        Returns:
            The new name of the job template.
        """
        return self._new_name

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, RenameJobTemplate) and self._new_name == other._new_name
        )

    def __hash__(self) -> int:
        return hash(self._new_name)

    def __str__(self) -> str:
        return f"RENAME JOB TEMPLATE {self._new_name}"


class UpdateJobTemplateComment(JobTemplateChange):
    """
    A job template change to update the comment of the job template.
    """

    def __init__(self, new_comment: str):
        """
        Initialize an UpdateJobTemplateComment change.

        Args:
            new_comment: The new comment of the job template.
        """
        self._new_comment = new_comment

    def get_new_comment(self) -> str:
        """
        Get the new comment of the job template.

        Returns:
            The new comment of the job template.
        """
        return self._new_comment

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, UpdateJobTemplateComment)
            and self._new_comment == other._new_comment
        )

    def __hash__(self) -> int:
        return hash(self._new_comment)

    def __str__(self) -> str:
        return f"UPDATE JOB TEMPLATE COMMENT {self._new_comment}"


class UpdateJobTemplate(JobTemplateChange):
    """
    A job template change to update the details of the job template.
    """

    def __init__(self, template_update: "TemplateUpdate"):
        """
        Initialize an UpdateJobTemplate change.

        Args:
            template_update: The job template update details.
        """
        self._template_update = template_update

    def get_template_update(self) -> "TemplateUpdate":
        """
        Get the job template update.

        Returns:
            The job template update.
        """
        return self._template_update

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, UpdateJobTemplate)
            and self._template_update == other._template_update
        )

    def __hash__(self) -> int:
        return hash(self._template_update)

    def __str__(self) -> str:
        return f"UPDATE JOB TEMPLATE {type(self._template_update).__name__}"


class TemplateUpdate(ABC):
    """
    Base class for template updates.
    """

    def __init__(
        self,
        new_executable: str,
        new_arguments: Optional[List[str]] = None,
        new_environments: Optional[Dict[str, str]] = None,
        new_custom_fields: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize a TemplateUpdate.

        Args:
            new_executable: The new executable of the job template.
            new_arguments: The new arguments of the job template.
            new_environments: The new environments of the job template.
            new_custom_fields: The new custom fields of the job template.
        """
        if not new_executable or not new_executable.strip():
            raise ValueError("Executable cannot be null or blank")
        self._new_executable = new_executable
        self._new_arguments = (
            deepcopy(new_arguments) if new_arguments is not None else []
        )
        self._new_environments = (
            deepcopy(new_environments) if new_environments is not None else {}
        )
        self._new_custom_fields = (
            deepcopy(new_custom_fields) if new_custom_fields is not None else {}
        )

    def get_new_executable(self) -> str:
        """
        Get the new executable of the job template.

        Returns:
            The new executable of the job template.
        """
        return self._new_executable

    def get_new_arguments(self) -> List[str]:
        """
        Get the new arguments of the job template.

        Returns:
            The new arguments of the job template.
        """
        return self._new_arguments

    def get_new_environments(self) -> Dict[str, str]:
        """
        Get the new environments of the job template.

        Returns:
            The new environments of the job template.
        """
        return self._new_environments

    def get_new_custom_fields(self) -> Dict[str, str]:
        """
        Get the new custom fields of the job template.

        Returns:
            The new custom fields of the job template.
        """
        return self._new_custom_fields

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, TemplateUpdate):
            return False
        return (
            self._new_executable == other._new_executable
            and self._new_arguments == other._new_arguments
            and self._new_environments == other._new_environments
            and self._new_custom_fields == other._new_custom_fields
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._new_executable,
                tuple(self._new_arguments),
                frozenset(self._new_environments.items()),
                frozenset(self._new_custom_fields.items()),
            )
        )


class ShellTemplateUpdate(TemplateUpdate):
    """
    A template update for shell job templates.
    """

    def __init__(
        self,
        new_executable: str,
        new_arguments: Optional[List[str]] = None,
        new_environments: Optional[Dict[str, str]] = None,
        new_custom_fields: Optional[Dict[str, str]] = None,
        new_scripts: Optional[List[str]] = None,
    ):
        """
        Initialize a ShellTemplateUpdate.

        Args:
            new_executable: The new executable of the shell job template.
            new_arguments: The new arguments of the shell job template.
            new_environments: The new environments of the shell job template.
            new_custom_fields: The new custom fields of the shell job template.
            new_scripts: The new scripts of the shell job template.
        """
        super().__init__(
            new_executable, new_arguments, new_environments, new_custom_fields
        )
        self._new_scripts = deepcopy(new_scripts) if new_scripts is not None else []

    def get_new_scripts(self) -> List[str]:
        """
        Get the new scripts of the shell job template.

        Returns:
            The new scripts of the shell job template.
        """
        return self._new_scripts

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ShellTemplateUpdate)
            and super().__eq__(other)
            and self._new_scripts == other._new_scripts
        )

    def __hash__(self) -> int:
        return hash((super().__hash__(), tuple(self._new_scripts)))


class SparkTemplateUpdate(TemplateUpdate):
    """
    A template update for spark job templates.
    """

    def __init__(
        self,
        new_executable: str,
        new_arguments: Optional[List[str]] = None,
        new_environments: Optional[Dict[str, str]] = None,
        new_custom_fields: Optional[Dict[str, str]] = None,
        new_class_name: Optional[str] = None,
        new_jars: Optional[List[str]] = None,
        new_files: Optional[List[str]] = None,
        new_archives: Optional[List[str]] = None,
        new_configs: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize a SparkTemplateUpdate.

        Args:
            new_executable: The new executable of the spark job template.
            new_arguments: The new arguments of the spark job template.
            new_environments: The new environments of the spark job template.
            new_custom_fields: The new custom fields of the spark job template.
            new_class_name: The new class name of the spark job template.
            new_jars: The new jars of the spark job template.
            new_files: The new files of the spark job template.
            new_archives: The new archives of the spark job template.
            new_configs: The new configs of the spark job template.
        """
        super().__init__(
            new_executable, new_arguments, new_environments, new_custom_fields
        )
        self._new_class_name = new_class_name
        self._new_jars = deepcopy(new_jars) if new_jars is not None else []
        self._new_files = deepcopy(new_files) if new_files is not None else []
        self._new_archives = deepcopy(new_archives) if new_archives is not None else []
        self._new_configs = deepcopy(new_configs) if new_configs is not None else {}

    def get_new_class_name(self) -> Optional[str]:
        """
        Get the new class name of the spark job template.

        Returns:
            The new class name of the spark job template.
        """
        return self._new_class_name

    def get_new_jars(self) -> List[str]:
        """
        Get the new jars of the spark job template.

        Returns:
            The new jars of the spark job template.
        """
        return self._new_jars

    def get_new_files(self) -> List[str]:
        """
        Get the new files of the spark job template.

        Returns:
            The new files of the spark job template.
        """
        return self._new_files

    def get_new_archives(self) -> List[str]:
        """
        Get the new archives of the spark job template.

        Returns:
            The new archives of the spark job template.
        """
        return self._new_archives

    def get_new_configs(self) -> Dict[str, str]:
        """
        Get the new configs of the spark job template.

        Returns:
            The new configs of the spark job template.
        """
        return self._new_configs

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, SparkTemplateUpdate)
            and super().__eq__(other)
            and self._new_class_name == other._new_class_name
            and self._new_jars == other._new_jars
            and self._new_files == other._new_files
            and self._new_archives == other._new_archives
            and self._new_configs == other._new_configs
        )

    def __hash__(self) -> int:
        return hash(
            (
                super().__hash__(),
                self._new_class_name,
                tuple(self._new_jars),
                tuple(self._new_files),
                tuple(self._new_archives),
                frozenset(self._new_configs.items()),
            )
        )
