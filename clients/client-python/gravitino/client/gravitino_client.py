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

from typing import List, Dict

from gravitino.api.catalog import Catalog
from gravitino.api.catalog_change import CatalogChange
from gravitino.api.job.job_handle import JobHandle
from gravitino.api.job.job_template import JobTemplate
from gravitino.api.job.supports_jobs import SupportsJobs
from gravitino.auth.auth_data_provider import AuthDataProvider
from gravitino.client.gravitino_client_base import GravitinoClientBase
from gravitino.client.gravitino_metalake import GravitinoMetalake


class GravitinoClient(GravitinoClientBase, SupportsJobs):
    """Gravitino Client for a user to interact with the Gravitino API, allowing the client to list,
    load, create, and alter Catalog.

    It uses an underlying {@link RESTClient} to send HTTP requests and receive responses from the API.
    """

    _metalake: GravitinoMetalake

    def __init__(
        self,
        uri: str,
        metalake_name: str,
        check_version: bool = True,
        auth_data_provider: AuthDataProvider = None,
        request_headers: dict = None,
        client_config: dict = None,
    ):
        """Constructs a new GravitinoClient with the given URI, authenticator and AuthDataProvider.

        Args:
            uri: The base URI for the Gravitino API.
            metalake_name: The specified metalake name.
            auth_data_provider: The provider of the data which is used for authentication.
            request_headers: The headers to be included in the HTTP requests.
            client_config: The config properties for the HTTP Client

        Raises:
            NoSuchMetalakeException if the metalake with specified name does not exist.
        """
        super().__init__(
            uri, check_version, auth_data_provider, request_headers, client_config
        )
        self.check_metalake_name(metalake_name)
        self._metalake = super().load_metalake(metalake_name)

    def get_metalake(self) -> GravitinoMetalake:
        """Get the current metalake object

        Raises:
            NoSuchMetalakeException if the metalake with specified name does not exist.

        Returns:
            the GravitinoMetalake object
        """
        return self._metalake

    def list_catalogs(self) -> List[str]:
        return self.get_metalake().list_catalogs()

    def list_catalogs_info(self) -> List[Catalog]:
        return self.get_metalake().list_catalogs_info()

    def load_catalog(self, name: str) -> Catalog:
        return self.get_metalake().load_catalog(name)

    def create_catalog(
        self,
        name: str,
        catalog_type: Catalog.Type,
        provider: str,
        comment: str,
        properties: Dict[str, str],
    ) -> Catalog:
        return self.get_metalake().create_catalog(
            name, catalog_type, provider, comment, properties
        )

    def alter_catalog(self, name: str, *changes: CatalogChange):
        return self.get_metalake().alter_catalog(name, *changes)

    def drop_catalog(self, name: str, force: bool = False) -> bool:
        return self.get_metalake().drop_catalog(name, force)

    def enable_catalog(self, name: str):
        return self.get_metalake().enable_catalog(name)

    def disable_catalog(self, name: str):
        return self.get_metalake().disable_catalog(name)

    def list_job_templates(self) -> List[JobTemplate]:
        """Lists all job templates in the current metalake.

        Returns:
            A list of JobTemplate objects representing the job templates in the metalake.
        """
        return self.get_metalake().list_job_templates()

    def register_job_template(self, job_template) -> None:
        """Register a job template with the specified job template to Gravitino. The registered
        job template will be maintained in Gravitino, allowing it to be executed later.

        Args:
            job_template: The job template to register.

        Raises:
            JobTemplateAlreadyExists: If a job template with the same name already exists.
        """
        self.get_metalake().register_job_template(job_template)

    def get_job_template(self, job_template_name: str) -> JobTemplate:
        """Retrieves a job template by its name.

        Args:
            job_template_name: The name of the job template to retrieve.

        Returns:
            The JobTemplate object corresponding to the specified name.

        Raises:
            NoSuchJobTemplateException: If no job template with the specified name exists.
        """
        return self.get_metalake().get_job_template(job_template_name)

    def delete_job_template(self, job_template_name: str) -> bool:
        """
        Deletes a job template by its name. This will remove the job template from Gravitino, and it
        will no longer be available for execution. Only when all the jobs associated with this job
        template are completed, failed or cancelled, the job template can be deleted successfully,
        otherwise it will throw InUseException. Returns false if the job template to be deleted does
        not exist.

        The deletion of a job template will also delete all the jobs associated with this template.

        Args:
            job_template_name: The name of the job template to delete.

        Returns:
            bool: True if the job template was deleted successfully, False if the job template
            does not exist.

        Raises:
            InUseException: If there are still queued or started jobs associated with this job template.
        """
        return self.get_metalake().delete_job_template(job_template_name)

    def list_jobs(self, job_template_name: str = None) -> List[JobHandle]:
        """Lists all the jobs in the current metalake.

        Args:
            job_template_name: Optional; if provided, filters the jobs by the specified job template name.

        Returns:
            A list of JobHandle objects representing the jobs in the metalake.
        """
        return self.get_metalake().list_jobs(job_template_name)

    def get_job(self, job_id: str) -> JobHandle:
        """Retrieves a job by its ID.

        Args:
            job_id: The ID of the job to retrieve.

        Returns:
            The JobHandle object corresponding to the specified job ID.

        Raises:
            NoSuchJobException: If no job with the specified ID exists.
        """
        return self.get_metalake().get_job(job_id)

    def run_job(self, job_template_name: str, job_conf: Dict[str, str]) -> JobHandle:
        """Runs a job using the specified job template and configuration.

        Args:
            job_template_name: The name of the job template to use for running the job.
            job_conf: A dictionary containing the configuration for the job.

        Returns:
            A JobHandle object representing the started job.

        Raises:
            NoSuchJobTemplateException: If no job template with the specified name exists.
        """
        return self.get_metalake().run_job(job_template_name, job_conf)

    def cancel_job(self, job_id: str) -> JobHandle:
        """Cancels a job by its ID.

        Args:
            job_id: The ID of the job to cancel.

        Returns:
            The JobHandle object representing the cancelled job.

        Raises:
            NoSuchJobException: If no job with the specified ID exists.
        """
        return self.get_metalake().cancel_job(job_id)
