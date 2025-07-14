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

from abc import ABC, abstractmethod
from typing import List, Dict
from .job_handle import JobHandle
from .job_template import JobTemplate


class SupportsJobs(ABC):
    """
    Interface for job management operations. This interface will be mixed with GravitinoClient to
    provide the ability to manage job templates and jobs within the Gravitino system.
    """

    @abstractmethod
    def list_job_templates(self) -> List[JobTemplate]:
        """
        Lists all the registered job templates in Gravitino.

        Returns:
            List of job templates.
        """
        pass

    @abstractmethod
    def register_job_template(self, job_template) -> None:
        """
        Register a job template with the specified job template to Gravitino. The registered
        job will be maintained in Gravitino, allowing it to be executed later.

        Args:
            job_template: The job template to register.

        Raises:
            JobTemplateAlreadyExists: If a job template with the same name already exists.
        """
        pass

    @abstractmethod
    def get_job_template(self, job_template_name: str) -> JobTemplate:
        """
        Retrieves a job template by its name.

        Args:
            job_template_name: The name of the job template to retrieve.

        Returns:
            The job template if found, otherwise raises an exception.

        Raises:
            NoSuchJobTemplateException: If no job template with the specified name exists.
        """
        pass

    @abstractmethod
    def delete_job_template(self, job_template_name: str) -> bool:
        """
        Deletes a job template by its name. This will remove the job template from Gravitino, and it
        will no longer be available for execution. Only when all the jobs associated with this job
        template are completed, failed or cancelled, the job template can be deleted successfully,
        otherwise it will return false.

        The deletion of a job template will also delete all the jobs associated with this template.

        Args:
            job_template_name: The name of the job template to delete.

        Returns:
            bool: True if the job template was deleted successfully, False if the job template
            does not exist.

        Raises:
            InUseException: If there are still queued or started jobs associated with this job template.
        """
        pass

    def list_jobs(self, job_template_name: str = None) -> List[JobHandle]:
        """
        Lists all the jobs in Gravitino. If a job template name is provided, it will filter the jobs
        by that template.

        Args:
            job_template_name: Optional; if provided, only jobs associated with this template will be listed.

        Returns:
            List of JobHandle objects representing the jobs.
        """
        pass

    @abstractmethod
    def run_job(self, job_template_name: str, job_conf: Dict[str, str]) -> JobHandle:
        """
        Run a job with the template name and configuration. The jobConf is a map of key-value
        contains the variables that will be used to replace the templated parameters in the job
        template.

        Args:
            job_template_name: The name of the job template to run.
            job_conf: A dictionary containing the job configuration parameters.

        Returns:
            JobHandle: A handle to the running job, which can be used to monitor or manage the job.

        Raises:
            NoSuchJobTemplateException: If no job template with the specified name exists.
        """
        pass

    @abstractmethod
    def get_job(self, job_id: str) -> JobHandle:
        """
        Retrieves a job by its ID.

        Args:
            job_id: The ID of the job to retrieve.

        Returns:
            JobHandle: The handle representing the job with the specified ID.

        Raises:
            NoSuchJobException: If no job with the specified ID exists.
        """
        pass

    @abstractmethod
    def cancel_job(self, job_id: str) -> JobHandle:
        """
        Cancels a running job by its ID. This operation will attempt to cancel the job if it is
        still running. This method will return immediately, user could use the job handle to
        check the status of the job after invoking this method.

        Args:
            job_id: The ID of the job to cancel.

        Raises:
            NoSuchJobException: If no job with the specified ID exists.
        """
        pass
