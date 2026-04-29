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

import logging
from typing import Dict, List

from gravitino.api.authorization.group import Group
from gravitino.api.authorization.privileges import Privilege
from gravitino.api.authorization.role import Role
from gravitino.api.authorization.securable_objects import SecurableObject
from gravitino.api.authorization.user import User
from gravitino.api.catalog import Catalog
from gravitino.api.catalog_change import CatalogChange
from gravitino.api.job.job_handle import JobHandle
from gravitino.api.job.job_template import JobTemplate
from gravitino.api.job.job_template_change import JobTemplateChange
from gravitino.api.job.supports_jobs import SupportsJobs
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.tag.tag import Tag
from gravitino.api.tag.tag_operations import TagOperations
from gravitino.client.dto_converters import DTOConverters
from gravitino.client.generic_job_handle import GenericJobHandle
from gravitino.client.generic_tag import GenericTag
from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.dto.requests.catalog_create_request import CatalogCreateRequest
from gravitino.dto.requests.catalog_set_request import CatalogSetRequest
from gravitino.dto.requests.catalog_updates_request import CatalogUpdatesRequest
from gravitino.dto.requests.group_add_request import GroupAddRequest
from gravitino.dto.requests.job_run_request import JobRunRequest
from gravitino.dto.requests.job_template_register_request import (
    JobTemplateRegisterRequest,
)
from gravitino.dto.requests.job_template_updates_request import (
    JobTemplateUpdatesRequest,
)
from gravitino.dto.requests.privilege_grant_request import PrivilegeGrantRequest
from gravitino.dto.requests.privilege_revoke_request import PrivilegeRevokeRequest
from gravitino.dto.requests.role_create_request import RoleCreateRequest
from gravitino.dto.requests.role_grant_request import RoleGrantRequest
from gravitino.dto.requests.role_revoke_request import RoleRevokeRequest
from gravitino.dto.requests.tag_create_request import TagCreateRequest
from gravitino.dto.requests.tag_updates_request import TagUpdatesRequest
from gravitino.dto.requests.user_add_request import UserAddRequest
from gravitino.dto.responses.catalog_list_response import CatalogListResponse
from gravitino.dto.responses.catalog_response import CatalogResponse
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.remove_response import RemoveResponse
from gravitino.dto.responses.entity_list_response import EntityListResponse
from gravitino.dto.responses.group_response import (
    GroupListResponse,
    GroupNamesListResponse,
    GroupResponse,
)
from gravitino.dto.responses.job_list_response import JobListResponse
from gravitino.dto.responses.job_response import JobResponse
from gravitino.dto.responses.job_template_list_response import JobTemplateListResponse
from gravitino.dto.responses.job_template_response import JobTemplateResponse
from gravitino.dto.responses.role_response import RoleNamesListResponse, RoleResponse
from gravitino.dto.responses.tag_response import (
    TagListResponse,
    TagNamesListResponse,
    TagResponse,
)
from gravitino.dto.responses.user_response import (
    UserListResponse,
    UserNamesListResponse,
    UserResponse,
)
from gravitino.exceptions.handlers.authorization_error_handler import (
    AUTHORIZATION_ERROR_HANDLER,
)
from gravitino.exceptions.handlers.catalog_error_handler import CATALOG_ERROR_HANDLER
from gravitino.exceptions.handlers.job_error_handler import JOB_ERROR_HANDLER
from gravitino.exceptions.handlers.tag_error_handler import TAG_ERROR_HANDLER
from gravitino.rest.rest_utils import encode_string
from gravitino.utils.http_client import HTTPClient
from gravitino.utils.precondition import Precondition
from gravitino.utils.string_utils import StringUtils

logger = logging.getLogger(__name__)


class GravitinoMetalake(
    MetalakeDTO,
    SupportsJobs,
    TagOperations,
):
    """
    Gravitino Metalake is the top-level metadata repository for users. It contains a list of catalogs
    as sub-level metadata collections. With GravitinoMetalake, users can list, create, load,
    alter and drop a catalog with specified identifier.
    """

    rest_client: HTTPClient

    API_METALAKES_CATALOGS_PATH = "api/metalakes/{}/catalogs/{}"
    API_METALAKES_JOB_TEMPLATES_PATH = "api/metalakes/{}/jobs/templates"
    API_METALAKES_JOB_RUNS_PATH = "api/metalakes/{}/jobs/runs"
    API_METALAKES_TAG_PATH = "api/metalakes/{}/tags/{}"
    API_METALAKES_TAGS_PATH = "api/metalakes/{}/tags"

    # Authorization paths
    API_METALAKES_USERS_PATH = "api/metalakes/{}/users"
    API_METALAKES_USER_PATH = "api/metalakes/{}/users/{}"
    API_METALAKES_GROUPS_PATH = "api/metalakes/{}/groups"
    API_METALAKES_GROUP_PATH = "api/metalakes/{}/groups/{}"
    API_METALAKES_ROLES_PATH = "api/metalakes/{}/roles"
    API_METALAKES_ROLE_PATH = "api/metalakes/{}/roles/{}"
    API_PERMISSIONS_USER_GRANT_PATH = "api/metalakes/{}/permissions/users/{}/grant"
    API_PERMISSIONS_USER_REVOKE_PATH = "api/metalakes/{}/permissions/users/{}/revoke"
    API_PERMISSIONS_GROUP_GRANT_PATH = "api/metalakes/{}/permissions/groups/{}/grant"
    API_PERMISSIONS_GROUP_REVOKE_PATH = "api/metalakes/{}/permissions/groups/{}/revoke"
    API_PERMISSIONS_ROLE_GRANT_PATH = (
        "api/metalakes/{}/permissions/roles/{}/{}/{}/grant"
    )
    API_PERMISSIONS_ROLE_REVOKE_PATH = (
        "api/metalakes/{}/permissions/roles/{}/{}/{}/revoke"
    )
    API_METALAKES_OBJECT_ROLES_PATH = "api/metalakes/{}/objects/{}/{}/roles"

    def __init__(self, metalake: MetalakeDTO = None, client: HTTPClient = None):
        super().__init__(
            _name=metalake.name(),
            _comment=metalake.comment(),
            _properties=metalake.properties(),
            _audit=metalake.audit_info(),
        )
        self.rest_client = client

    def list_catalogs(self) -> List[str]:
        """List all the catalogs under this metalake.

        Raises:
            NoSuchMetalakeException if the metalake with specified namespace does not exist.

        Returns:
            A list of the catalog names under this metalake.
        """
        url = f"api/metalakes/{encode_string(self.name())}/catalogs"
        response = self.rest_client.get(url, error_handler=CATALOG_ERROR_HANDLER)
        entity_list = EntityListResponse.from_json(response.body, infer_missing=True)
        entity_list.validate()
        return [identifier.name() for identifier in entity_list.identifiers()]

    def list_catalogs_info(self) -> List[Catalog]:
        """List all the catalogs with their information under this metalake.

        Raises:
            NoSuchMetalakeException if the metalake with specified namespace does not exist.

        Returns:
            A list of Catalog under the specified namespace.
        """
        params = {"details": "true"}
        url = f"api/metalakes/{encode_string(self.name())}/catalogs"
        response = self.rest_client.get(
            url, params=params, error_handler=CATALOG_ERROR_HANDLER
        )
        catalog_list = CatalogListResponse.from_json(response.body, infer_missing=True)

        return [
            DTOConverters.to_catalog(self.name(), catalog, self.rest_client)
            for catalog in catalog_list.catalogs()
        ]

    def load_catalog(self, name: str) -> Catalog:
        """Load the catalog with specified name.

        Args:
            name: The name of the catalog to load.

        Raises:
            NoSuchCatalogException if the catalog with specified name does not exist.

        Returns:
            The Catalog with specified name.
        """
        url = self.API_METALAKES_CATALOGS_PATH.format(
            encode_string(self.name()), encode_string(name)
        )
        response = self.rest_client.get(url, error_handler=CATALOG_ERROR_HANDLER)
        catalog_resp = CatalogResponse.from_json(response.body, infer_missing=True)

        return DTOConverters.to_catalog(
            self.name(), catalog_resp.catalog(), self.rest_client
        )

    def create_catalog(
        self,
        name: str,
        catalog_type: Catalog.Type,
        provider: str,
        comment: str,
        properties: Dict[str, str],
    ) -> Catalog:
        """Create a new catalog with specified name, catalog type, comment and properties.

        Args:
            name: The name of the catalog.
            catalog_type: The type of the catalog.
            provider: The provider of the catalog. This parameter can be None if the catalog
            provides a managed implementation. Currently, the model and fileset catalog support
            None provider. For the details, please refer to the Catalog.Type.
            comment: The comment of the catalog.
            properties: The properties of the catalog.

        Raises:
            NoSuchMetalakeException if the metalake does not exist.
            CatalogAlreadyExistsException if the catalog with specified name already exists.

        Returns:
            The created Catalog.
        """

        catalog_create_request = CatalogCreateRequest(
            name=name,
            catalog_type=catalog_type,
            provider=provider,
            comment=comment,
            properties=properties,
        )
        catalog_create_request.validate()

        url = f"api/metalakes/{encode_string(self.name())}/catalogs"
        response = self.rest_client.post(
            url, json=catalog_create_request, error_handler=CATALOG_ERROR_HANDLER
        )
        catalog_resp = CatalogResponse.from_json(response.body, infer_missing=True)

        return DTOConverters.to_catalog(
            self.name(), catalog_resp.catalog(), self.rest_client
        )

    def alter_catalog(self, name: str, *changes: CatalogChange) -> Catalog:
        """Alter the catalog with specified name by applying the changes.

        Args:
            name: the name of the catalog.
            changes: the changes to apply to the catalog.

        Raises:
            NoSuchCatalogException if the catalog with specified name does not exist.
            IllegalArgumentException if the changes are invalid.

        Returns:
            the altered Catalog.
        """

        reqs = [DTOConverters.to_catalog_update_request(change) for change in changes]
        updates_request = CatalogUpdatesRequest(reqs)
        updates_request.validate()

        url = self.API_METALAKES_CATALOGS_PATH.format(
            encode_string(self.name()), encode_string(name)
        )
        response = self.rest_client.put(
            url, json=updates_request, error_handler=CATALOG_ERROR_HANDLER
        )
        catalog_response = CatalogResponse.from_json(response.body, infer_missing=True)
        catalog_response.validate()

        return DTOConverters.to_catalog(
            self.name(), catalog_response.catalog(), self.rest_client
        )

    def drop_catalog(self, name: str, force: bool = False) -> bool:
        """Drop the catalog with specified name.

        Args:
            name: the name of the catalog.
            force: whether to force drop the catalog.

        Returns:
            true if the catalog is dropped successfully, false if the catalog does not exist.
        """
        params = {"force": str(force)}
        url = self.API_METALAKES_CATALOGS_PATH.format(
            encode_string(self.name()), encode_string(name)
        )
        response = self.rest_client.delete(
            url, params=params, error_handler=CATALOG_ERROR_HANDLER
        )

        drop_response = DropResponse.from_json(response.body, infer_missing=True)
        drop_response.validate()

        return drop_response.dropped()

    def enable_catalog(self, name: str):
        """Enable the catalog with specified name. If the catalog is already in use, this method does nothing.

        Args:
            name: the name of the catalog.

        Raises:
            NoSuchCatalogException if the catalog with specified name does not exist.
        """

        catalog_enable_request = CatalogSetRequest(in_use=True)
        catalog_enable_request.validate()

        url = self.API_METALAKES_CATALOGS_PATH.format(
            encode_string(self.name()), encode_string(name)
        )
        self.rest_client.patch(
            url, json=catalog_enable_request, error_handler=CATALOG_ERROR_HANDLER
        )

    def disable_catalog(self, name: str):
        """Disable the catalog with specified name. If the catalog is already disabled, this method does nothing.

        Args:
            name: the name of the catalog.

        Raises:
            NoSuchCatalogException if the catalog with specified name does not exist.
        """

        catalog_disable_request = CatalogSetRequest(in_use=False)
        catalog_disable_request.validate()

        url = self.API_METALAKES_CATALOGS_PATH.format(
            encode_string(self.name()), encode_string(name)
        )
        self.rest_client.patch(
            url, json=catalog_disable_request, error_handler=CATALOG_ERROR_HANDLER
        )

    ##########
    # Job operations
    ##########

    def list_job_templates(self) -> List[JobTemplate]:
        """List all the registered job templates in Gravitino.

        Returns:
            List of job templates.
        """
        params = {"details": "true"}
        url = self.API_METALAKES_JOB_TEMPLATES_PATH.format(encode_string(self.name()))
        response = self.rest_client.get(
            url, params=params, error_handler=JOB_ERROR_HANDLER
        )
        resp = JobTemplateListResponse.from_json(response.body, infer_missing=True)
        resp.validate()

        return [
            DTOConverters.from_job_template_dto(dto) for dto in resp.job_templates()
        ]

    def register_job_template(self, job_template: JobTemplate) -> None:
        """Register a job template with the specified job template to Gravitino. The registered
        job template will be maintained in Gravitino, allowing it to be executed later.

        Args:
            job_template: The job template to register.

        Raises:
            JobTemplateAlreadyExists: If a job template with the same name already exists.
        """
        url = self.API_METALAKES_JOB_TEMPLATES_PATH.format(encode_string(self.name()))
        req = JobTemplateRegisterRequest(
            DTOConverters.to_job_template_dto(job_template)
        )

        self.rest_client.post(url, json=req, error_handler=JOB_ERROR_HANDLER)

    def get_job_template(self, job_template_name: str) -> JobTemplate:
        """Retrieves a job template by its name.

        Args:
            job_template_name: The name of the job template to retrieve.

        Returns:
            The job template if found, otherwise raises an exception.

        Raises:
            NoSuchJobTemplateException: If no job template with the specified name exists.
        """
        if not job_template_name or not job_template_name.strip():
            raise ValueError("Job template name cannot be null or empty")

        url = (
            f"{self.API_METALAKES_JOB_TEMPLATES_PATH.format(encode_string(self.name()))}/"
            f"{encode_string(job_template_name)}"
        )
        response = self.rest_client.get(url, error_handler=JOB_ERROR_HANDLER)
        resp = JobTemplateResponse.from_json(response.body, infer_missing=True)
        resp.validate()

        return DTOConverters.from_job_template_dto(resp.job_template())

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
            InUseException: If the job template is currently in use by any jobs, it cannot be deleted.
        """
        if not job_template_name or not job_template_name.strip():
            raise ValueError("Job template name cannot be null or empty")

        url = (
            f"{self.API_METALAKES_JOB_TEMPLATES_PATH.format(encode_string(self.name()))}/"
            f"{encode_string(job_template_name)}"
        )
        response = self.rest_client.delete(url, error_handler=JOB_ERROR_HANDLER)

        drop_response = DropResponse.from_json(response.body, infer_missing=True)
        drop_response.validate()

        return drop_response.dropped()

    def alter_job_template(
        self, job_template_name: str, *changes: JobTemplateChange
    ) -> JobTemplate:
        """Alter the job template with specified name by applying the changes.

        Args:
            job_template_name: the name of the job template.
            changes: the changes to apply to the job template.

        Raises:
            NoSuchJobTemplateException if the job template with specified name does not exist.
            IllegalArgumentException if the changes are invalid.

        Returns:
            the altered JobTemplate.
        """

        reqs = [
            DTOConverters.to_job_template_update_request(change) for change in changes
        ]
        updates_request = JobTemplateUpdatesRequest(reqs)

        url = (
            f"{self.API_METALAKES_JOB_TEMPLATES_PATH.format(encode_string(self.name()))}/"
            f"{encode_string(job_template_name)}"
        )
        response = self.rest_client.put(
            url, json=updates_request, error_handler=JOB_ERROR_HANDLER
        )
        job_template_response = JobTemplateResponse.from_json(
            response.body, infer_missing=True
        )
        job_template_response.validate()

        return DTOConverters.from_job_template_dto(job_template_response.job_template())

    def list_jobs(self, job_template_name: str = None) -> List[JobHandle]:
        """List all the jobs under this metalake.

        Args:
            job_template_name: The name of the job template to filter jobs by. If None, all jobs are listed.

        Returns:
            A list of JobHandle objects representing the jobs.
        """
        params = {"jobTemplateName": job_template_name} if job_template_name else {}
        url = self.API_METALAKES_JOB_RUNS_PATH.format(encode_string(self.name()))
        response = self.rest_client.get(
            url, params=params, error_handler=JOB_ERROR_HANDLER
        )
        resp = JobListResponse.from_json(response.body, infer_missing=True)
        resp.validate()

        return [GenericJobHandle(dto) for dto in resp.jobs()]

    def get_job(self, job_id: str) -> JobHandle:
        """Retrieves a job by its ID.

        Args:
            job_id: The ID of the job to retrieve.

        Returns:
            The JobHandle representing the job if found, otherwise raises an exception.

        Raises:
            NoSuchJobException: If no job with the specified ID exists.
        """
        if not job_id or not job_id.strip():
            raise ValueError("Job ID cannot be null or empty")

        url = (
            f"{self.API_METALAKES_JOB_RUNS_PATH.format(encode_string(self.name()))}"
            f"/{encode_string(job_id)}"
        )
        response = self.rest_client.get(url, error_handler=JOB_ERROR_HANDLER)
        resp = JobResponse.from_json(response.body, infer_missing=True)
        resp.validate()

        return GenericJobHandle(resp.job())

    def run_job(self, job_template_name: str, job_conf: Dict[str, str]) -> JobHandle:
        """Runs a job based on the specified job template and configuration.

        Args:
            job_template_name: The name of the job template to use for running the job.
            job_conf: A dictionary containing the configuration for the job.

        Returns:
            A JobHandle representing the started job.

        Raises:
            NoSuchJobTemplateException: If no job template with the specified name exists.
        """
        url = f"{self.API_METALAKES_JOB_RUNS_PATH.format(encode_string(self.name()))}"
        request = JobRunRequest(job_template_name, job_conf)

        response = self.rest_client.post(
            url, json=request, error_handler=JOB_ERROR_HANDLER
        )
        resp = JobResponse.from_json(response.body, infer_missing=True)
        resp.validate()

        return GenericJobHandle(resp.job())

    def cancel_job(self, job_id: str) -> JobHandle:
        """Cancels a job by its ID.

        Args:
            job_id: The ID of the job to cancel.

        Returns:
            A JobHandle representing the cancelled job.

        Raises:
            NoSuchJobException: If no job with the specified ID exists.
        """
        if not job_id or not job_id.strip():
            raise ValueError("Job ID cannot be null or empty")

        url = (
            f"{self.API_METALAKES_JOB_RUNS_PATH.format(encode_string(self.name()))}"
            f"/{encode_string(job_id)}"
        )
        response = self.rest_client.post(url, error_handler=JOB_ERROR_HANDLER)
        resp = JobResponse.from_json(response.body, infer_missing=True)
        resp.validate()

        return GenericJobHandle(resp.job())

    #########
    # Tag operations
    #########
    def list_tags(self) -> list[str]:
        """List all the tag names under a metalake.

        Returns:
            list[str]: The list of tag names.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
        """
        url = self.API_METALAKES_TAGS_PATH.format(encode_string(self.name()))

        response = self.rest_client.get(url, error_handler=TAG_ERROR_HANDLER)
        resp = TagNamesListResponse.from_json(response.body, infer_missing=True)
        resp.validate()

        return resp.tag_names()

    def list_tags_info(self) -> List[Tag]:
        """
        List tags information under a metalake.

        Returns:
            list[Tag]: The list of tag information.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
        """
        url = self.API_METALAKES_TAGS_PATH.format(encode_string(self.name()))

        response = self.rest_client.get(
            url,
            params={"details": "true"},
            error_handler=TAG_ERROR_HANDLER,
        )
        list_info_resp = TagListResponse.from_json(response.body, infer_missing=True)
        list_info_resp.validate()

        return [
            GenericTag(
                self.name(),
                tag_dto,
                self.rest_client,
            )
            for tag_dto in list_info_resp.tags()
        ]

    def get_tag(self, tag_name) -> Tag:
        """
        Get a tag by its name under a metalake.

        Args:
            tag_name (str): The name of the tag.

        Returns:
            Tag: The tag information.

        Raises:
            NoSuchTagException: If the tag does not exist.
        """
        Precondition.check_string_not_empty(
            tag_name, "tag name must not be null or empty"
        )
        url = self.API_METALAKES_TAG_PATH.format(
            encode_string(self.name()), encode_string(tag_name)
        )
        response = self.rest_client.get(url, error_handler=TAG_ERROR_HANDLER)

        tag_resp = TagResponse.from_json(response.body, infer_missing=True)
        tag_resp.validate()

        return GenericTag(self.name(), tag_resp.tag(), self.rest_client)

    def create_tag(self, tag_name, comment, properties) -> Tag:
        """
        Create a new tag under a metalake.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
            TagAlreadyExistsException: If the tag already exists.

        Args:
            tag_name (str): The name of the tag.
            comment (str): The comment of the tag.
            properties (dict[str, str]): The properties of the tag.

        Returns:
            Tag: The tag information.
        """
        tag_create_request = TagCreateRequest(
            tag_name,
            comment,
            properties,
        )
        tag_create_request.validate()

        url = self.API_METALAKES_TAGS_PATH.format(encode_string(self.name()))

        response = self.rest_client.post(
            url,
            json=tag_create_request,
            error_handler=TAG_ERROR_HANDLER,
        )
        tag_resp = TagResponse.from_json(response.body, infer_missing=True)
        tag_resp.validate()

        return GenericTag(self.name(), tag_resp.tag(), self.rest_client)

    def alter_tag(self, tag_name, *changes) -> Tag:
        """
        Alter a tag under a metalake.

        Args:
            tag_name (str): The name of the tag.
            changes (TagChange): The changes to apply to the tag.

        Returns:
            Tag: The altered tag.

        Raises:
            NoSuchTagException: If the tag does not exist.
            NoSuchMetalakeException: If the metalake does not exist.
            IllegalArgumentException: If the changes cannot be applied to the tag.
            TagAlreadyExistsException: If a tag with the new name already exists.
        """
        Precondition.check_argument(
            StringUtils.is_not_blank(tag_name),
            "tag name must not be null or empty",
        )
        Precondition.check_argument(
            changes is not None and len(changes) > 0,
            "at least one change is required",
        )
        updates = [DTOConverters.to_tag_update_request(change) for change in changes]
        update_req = TagUpdatesRequest(updates)
        update_req.validate()

        url = self.API_METALAKES_TAG_PATH.format(
            encode_string(self.name()), encode_string(tag_name)
        )
        response = self.rest_client.put(
            url,
            json=update_req,
            error_handler=TAG_ERROR_HANDLER,
        )
        tag_resp: TagResponse = TagResponse.from_json(response.body, infer_missing=True)

        tag_resp.validate()
        return GenericTag(
            self.name(),
            tag_resp.tag(),
            self.rest_client,
        )

    def delete_tag(self, tag_name) -> bool:
        """
        Delete a tag under a metalake.

        Args:
            tag_name (str): The name of the tag.

        Returns:
            bool: True if the tag was deleted, False otherwise.

        Raises:
            NoSuchTagException: If the tag does not exist.
            NoSuchMetalakeException: If the metalake does not exist.
        """
        Precondition.check_argument(
            StringUtils.is_not_blank(tag_name),
            "tag name must not be null or empty",
        )

        url = self.API_METALAKES_TAG_PATH.format(
            encode_string(self.name()), encode_string(tag_name)
        )
        response = self.rest_client.delete(
            url,
            error_handler=TAG_ERROR_HANDLER,
        )
        drop_response = DropResponse.from_json(response.body, infer_missing=True)
        drop_response.validate()

        return drop_response.dropped()

    ####################
    # User operations
    ####################

    def add_user(self, user: str) -> User:
        """Add a user to this metalake."""
        Precondition.check_string_not_empty(user, "user name must not be null or empty")
        req = UserAddRequest(user)
        req.validate()
        url = self.API_METALAKES_USERS_PATH.format(encode_string(self.name()))
        response = self.rest_client.post(
            url, json=req, error_handler=AUTHORIZATION_ERROR_HANDLER
        )
        resp = UserResponse.from_json(response.body, infer_missing=True)
        resp.validate()
        return resp.user()

    def remove_user(self, user: str) -> bool:
        """Remove a user from this metalake."""
        Precondition.check_string_not_empty(user, "user name must not be null or empty")
        url = self.API_METALAKES_USER_PATH.format(
            encode_string(self.name()), encode_string(user)
        )
        response = self.rest_client.delete(
            url, error_handler=AUTHORIZATION_ERROR_HANDLER
        )
        remove_response = RemoveResponse.from_json(response.body, infer_missing=True)
        remove_response.validate()
        return remove_response.removed()

    def get_user(self, user: str) -> User:
        """Get a user by name from this metalake."""
        Precondition.check_string_not_empty(user, "user name must not be null or empty")
        url = self.API_METALAKES_USER_PATH.format(
            encode_string(self.name()), encode_string(user)
        )
        response = self.rest_client.get(url, error_handler=AUTHORIZATION_ERROR_HANDLER)
        resp = UserResponse.from_json(response.body, infer_missing=True)
        resp.validate()
        return resp.user()

    def list_users(self) -> list[User]:
        """List all users with details under this metalake."""
        url = self.API_METALAKES_USERS_PATH.format(encode_string(self.name()))
        response = self.rest_client.get(
            url, params={"details": "true"}, error_handler=AUTHORIZATION_ERROR_HANDLER
        )
        resp = UserListResponse.from_json(response.body, infer_missing=True)
        resp.validate()
        return resp.users()

    def list_user_names(self) -> list[str]:
        """List all user names under this metalake."""
        url = self.API_METALAKES_USERS_PATH.format(encode_string(self.name()))
        response = self.rest_client.get(url, error_handler=AUTHORIZATION_ERROR_HANDLER)
        resp = UserNamesListResponse.from_json(response.body, infer_missing=True)
        resp.validate()
        return resp.names()

    ####################
    # Group operations
    ####################

    def add_group(self, group: str) -> Group:
        """Add a group to this metalake."""
        Precondition.check_string_not_empty(
            group, "group name must not be null or empty"
        )
        req = GroupAddRequest(group)
        req.validate()
        url = self.API_METALAKES_GROUPS_PATH.format(encode_string(self.name()))
        response = self.rest_client.post(
            url, json=req, error_handler=AUTHORIZATION_ERROR_HANDLER
        )
        resp = GroupResponse.from_json(response.body, infer_missing=True)
        resp.validate()
        return resp.group()

    def remove_group(self, group: str) -> bool:
        """Remove a group from this metalake."""
        Precondition.check_string_not_empty(
            group, "group name must not be null or empty"
        )
        url = self.API_METALAKES_GROUP_PATH.format(
            encode_string(self.name()), encode_string(group)
        )
        response = self.rest_client.delete(
            url, error_handler=AUTHORIZATION_ERROR_HANDLER
        )
        remove_response = RemoveResponse.from_json(response.body, infer_missing=True)
        remove_response.validate()
        return remove_response.removed()

    def get_group(self, group: str) -> Group:
        """Get a group by name from this metalake."""
        Precondition.check_string_not_empty(
            group, "group name must not be null or empty"
        )
        url = self.API_METALAKES_GROUP_PATH.format(
            encode_string(self.name()), encode_string(group)
        )
        response = self.rest_client.get(url, error_handler=AUTHORIZATION_ERROR_HANDLER)
        resp = GroupResponse.from_json(response.body, infer_missing=True)
        resp.validate()
        return resp.group()

    def list_groups(self) -> list[Group]:
        """List all groups with details under this metalake."""
        url = self.API_METALAKES_GROUPS_PATH.format(encode_string(self.name()))
        response = self.rest_client.get(
            url, params={"details": "true"}, error_handler=AUTHORIZATION_ERROR_HANDLER
        )
        resp = GroupListResponse.from_json(response.body, infer_missing=True)
        resp.validate()
        return resp.groups()

    def list_group_names(self) -> list[str]:
        """List all group names under this metalake."""
        url = self.API_METALAKES_GROUPS_PATH.format(encode_string(self.name()))
        response = self.rest_client.get(url, error_handler=AUTHORIZATION_ERROR_HANDLER)
        resp = GroupNamesListResponse.from_json(response.body, infer_missing=True)
        resp.validate()
        return resp.names()

    ####################
    # Role operations
    ####################

    def create_role(
        self,
        role: str,
        properties: dict[str, str],
        securable_objects: list[SecurableObject],
    ) -> Role:
        """Create a new role in this metalake."""
        Precondition.check_string_not_empty(role, "role name must not be null or empty")
        securable_object_dtos = [
            DTOConverters.to_securable_object_dto(obj)
            for obj in (securable_objects or [])
        ]
        req = RoleCreateRequest(role, properties, securable_object_dtos)
        req.validate()
        url = self.API_METALAKES_ROLES_PATH.format(encode_string(self.name()))
        response = self.rest_client.post(
            url, json=req, error_handler=AUTHORIZATION_ERROR_HANDLER
        )
        resp = RoleResponse.from_json(response.body, infer_missing=True)
        resp.validate()
        return resp.role()

    def delete_role(self, role: str) -> bool:
        """Delete a role from this metalake."""
        Precondition.check_string_not_empty(role, "role name must not be null or empty")
        url = self.API_METALAKES_ROLE_PATH.format(
            encode_string(self.name()), encode_string(role)
        )
        response = self.rest_client.delete(
            url, error_handler=AUTHORIZATION_ERROR_HANDLER
        )
        drop_response = DropResponse.from_json(response.body, infer_missing=True)
        drop_response.validate()
        return drop_response.dropped()

    def get_role(self, role: str) -> Role:
        """Get a role by name from this metalake."""
        Precondition.check_string_not_empty(role, "role name must not be null or empty")
        url = self.API_METALAKES_ROLE_PATH.format(
            encode_string(self.name()), encode_string(role)
        )
        response = self.rest_client.get(url, error_handler=AUTHORIZATION_ERROR_HANDLER)
        resp = RoleResponse.from_json(response.body, infer_missing=True)
        resp.validate()
        return resp.role()

    def list_role_names(self) -> list[str]:
        """List all role names under this metalake."""
        url = self.API_METALAKES_ROLES_PATH.format(encode_string(self.name()))
        response = self.rest_client.get(url, error_handler=AUTHORIZATION_ERROR_HANDLER)
        resp = RoleNamesListResponse.from_json(response.body, infer_missing=True)
        resp.validate()
        return resp.names()

    ########################
    # Permission operations
    ########################

    def grant_roles_to_user(self, roles: list[str], user: str) -> User:
        """Grant roles to a user."""
        Precondition.check_string_not_empty(user, "user name must not be null or empty")
        req = RoleGrantRequest(roles)
        req.validate()
        url = self.API_PERMISSIONS_USER_GRANT_PATH.format(
            encode_string(self.name()), encode_string(user)
        )
        response = self.rest_client.put(
            url, json=req, error_handler=AUTHORIZATION_ERROR_HANDLER
        )
        resp = UserResponse.from_json(response.body, infer_missing=True)
        resp.validate()
        return resp.user()

    def revoke_roles_from_user(self, roles: list[str], user: str) -> User:
        """Revoke roles from a user."""
        Precondition.check_string_not_empty(user, "user name must not be null or empty")
        req = RoleRevokeRequest(roles)
        req.validate()
        url = self.API_PERMISSIONS_USER_REVOKE_PATH.format(
            encode_string(self.name()), encode_string(user)
        )
        response = self.rest_client.put(
            url, json=req, error_handler=AUTHORIZATION_ERROR_HANDLER
        )
        resp = UserResponse.from_json(response.body, infer_missing=True)
        resp.validate()
        return resp.user()

    def grant_roles_to_group(self, roles: list[str], group: str) -> Group:
        """Grant roles to a group."""
        Precondition.check_string_not_empty(
            group, "group name must not be null or empty"
        )
        req = RoleGrantRequest(roles)
        req.validate()
        url = self.API_PERMISSIONS_GROUP_GRANT_PATH.format(
            encode_string(self.name()), encode_string(group)
        )
        response = self.rest_client.put(
            url, json=req, error_handler=AUTHORIZATION_ERROR_HANDLER
        )
        resp = GroupResponse.from_json(response.body, infer_missing=True)
        resp.validate()
        return resp.group()

    def revoke_roles_from_group(self, roles: list[str], group: str) -> Group:
        """Revoke roles from a group."""
        Precondition.check_string_not_empty(
            group, "group name must not be null or empty"
        )
        req = RoleRevokeRequest(roles)
        req.validate()
        url = self.API_PERMISSIONS_GROUP_REVOKE_PATH.format(
            encode_string(self.name()), encode_string(group)
        )
        response = self.rest_client.put(
            url, json=req, error_handler=AUTHORIZATION_ERROR_HANDLER
        )
        resp = GroupResponse.from_json(response.body, infer_missing=True)
        resp.validate()
        return resp.group()

    def grant_privileges_to_role(
        self, role: str, obj: MetadataObject, privileges: list[Privilege]
    ) -> Role:
        """Grant privileges on a metadata object to a role."""
        Precondition.check_string_not_empty(role, "role name must not be null or empty")
        privilege_dtos = [DTOConverters.to_privilege_dto(p) for p in privileges]
        req = PrivilegeGrantRequest(privilege_dtos)
        req.validate()
        url = self.API_PERMISSIONS_ROLE_GRANT_PATH.format(
            encode_string(self.name()),
            encode_string(role),
            encode_string(obj.type().name),
            encode_string(obj.full_name()),
        )
        response = self.rest_client.put(
            url, json=req, error_handler=AUTHORIZATION_ERROR_HANDLER
        )
        resp = RoleResponse.from_json(response.body, infer_missing=True)
        resp.validate()
        return resp.role()

    def revoke_privileges_from_role(
        self, role: str, obj: MetadataObject, privileges: list[Privilege]
    ) -> Role:
        """Revoke privileges on a metadata object from a role."""
        Precondition.check_string_not_empty(role, "role name must not be null or empty")
        privilege_dtos = [DTOConverters.to_privilege_dto(p) for p in privileges]
        req = PrivilegeRevokeRequest(privilege_dtos)
        req.validate()
        url = self.API_PERMISSIONS_ROLE_REVOKE_PATH.format(
            encode_string(self.name()),
            encode_string(role),
            encode_string(obj.type().name),
            encode_string(obj.full_name()),
        )
        response = self.rest_client.put(
            url, json=req, error_handler=AUTHORIZATION_ERROR_HANDLER
        )
        resp = RoleResponse.from_json(response.body, infer_missing=True)
        resp.validate()
        return resp.role()

    ####################
    # Object role binding
    ####################

    def list_binding_role_names(
        self, type_: MetadataObject.Type, full_name: str
    ) -> list[str]:
        """List the role names bound to a metadata object."""
        url = self.API_METALAKES_OBJECT_ROLES_PATH.format(
            encode_string(self.name()),
            encode_string(type_.name),
            encode_string(full_name),
        )
        response = self.rest_client.get(url, error_handler=AUTHORIZATION_ERROR_HANDLER)
        resp = RoleNamesListResponse.from_json(response.body, infer_missing=True)
        resp.validate()
        return resp.names()
