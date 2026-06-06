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

from __future__ import annotations

from typing import Dict, List, Optional

from gravitino.api.authorization.group import Group
from gravitino.api.authorization.owner import Owner
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
from gravitino.api.tag.tag_operations import TagOperations
from gravitino.auth.auth_data_provider import AuthDataProvider
from gravitino.client.gravitino_client_base import GravitinoClientBase
from gravitino.client.gravitino_metalake import GravitinoMetalake

from ..api.tag.tag import Tag


class GravitinoClient(GravitinoClientBase, SupportsJobs, TagOperations):
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
        request_headers: Optional[dict] = None,
        client_config: Optional[dict] = None,
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

    def alter_job_template(
        self, job_template_name: str, *changes: JobTemplateChange
    ) -> JobTemplate:
        """Alters a job template with the specified changes.

        Args:
            job_template_name: The name of the job template to alter.
            changes: The changes to apply to the job template.

        Returns:
            The altered JobTemplate object.

        Raises:
            NoSuchJobTemplateException: If no job template with the specified name exists.
            IllegalArgumentException: If any of the changes cannot be applied.
        """
        return self.get_metalake().alter_job_template(job_template_name, *changes)

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

    # Tag operations
    def list_tags(self) -> list[str]:
        """List all the tag names under a metalake.

        Returns:
            list[str]: The list of tag names.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().list_tags()

    def list_tags_info(self) -> list[Tag]:
        """
        List tags information under a metalake.

        Returns:
            list[Tag]: The list of tag information.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().list_tags_info()

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
        return self.get_metalake().get_tag(tag_name)

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
        return self.get_metalake().create_tag(tag_name, comment, properties)

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
        return self.get_metalake().alter_tag(tag_name, *changes)

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
        return self.get_metalake().delete_tag(tag_name)

    # Owner operations
    def get_owner(self, metadata_object: MetadataObject) -> Optional[Owner]:
        """Get the owner of a metadata object.

        Args:
            metadata_object: The metadata object to get the owner for.

        Returns:
            Optional[Owner]: The owner of the metadata object, or None if no owner is set.

        Raises:
            NoSuchMetadataObjectException: If the metadata object does not exist.
            NotFoundException: If a related resource is not found.
            MetalakeNotInUseException: If the metalake is not in use.
        """
        return self.get_metalake().get_owner(metadata_object)

    def set_owner(
        self, metadata_object: MetadataObject, owner_name: str, owner_type: Owner.Type
    ) -> None:
        """Set the owner of a metadata object.

        Args:
            metadata_object: The metadata object to set the owner for.
            owner_name: The name of the owner.
            owner_type: The type of the owner (USER or GROUP).

        Raises:
            NoSuchMetadataObjectException: If the metadata object does not exist.
            NotFoundException: If a related resource is not found.
            MetalakeNotInUseException: If the metalake is not in use.
            UnsupportedOperationException: If the operation is not supported.
        """
        self.get_metalake().set_owner(metadata_object, owner_name, owner_type)

    # User operations

    def add_user(self, user: str) -> User:
        """Add a user to the metalake.

        Args:
            user: The name of the user.

        Returns:
            The added User object.

        Raises:
            UserAlreadyExistsException: If a user with the same name already exists.
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().add_user(user)

    def remove_user(self, user: str) -> bool:
        """Remove a user from the metalake.

        Args:
            user: The name of the user.

        Returns:
            True if the user was removed, False if the user did not exist.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().remove_user(user)

    def get_user(self, user: str) -> User:
        """Get a user by name from the metalake.

        Args:
            user: The name of the user.

        Returns:
            The User object.

        Raises:
            NoSuchUserException: If the user does not exist.
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().get_user(user)

    def list_users(self) -> list[User]:
        """List all users with details under the metalake.

        Returns:
            A list of User objects.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().list_users()

    def list_user_names(self) -> list[str]:
        """List all user names under the metalake.

        Returns:
            A list of user name strings.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().list_user_names()

    # Group operations

    def add_group(self, group: str) -> Group:
        """Add a group to the metalake.

        Args:
            group: The name of the group.

        Returns:
            The added Group object.

        Raises:
            GroupAlreadyExistsException: If a group with the same name already exists.
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().add_group(group)

    def remove_group(self, group: str) -> bool:
        """Remove a group from the metalake.

        Args:
            group: The name of the group.

        Returns:
            True if the group was removed, False if the group did not exist.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().remove_group(group)

    def get_group(self, group: str) -> Group:
        """Get a group by name from the metalake.

        Args:
            group: The name of the group.

        Returns:
            The Group object.

        Raises:
            NoSuchGroupException: If the group does not exist.
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().get_group(group)

    def list_groups(self) -> list[Group]:
        """List all groups with details under the metalake.

        Returns:
            A list of Group objects.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().list_groups()

    def list_group_names(self) -> list[str]:
        """List all group names under the metalake.

        Returns:
            A list of group name strings.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().list_group_names()

    # Role operations

    def create_role(
        self,
        role_name: str,
        properties: Optional[Dict[str, str]] = None,
        securable_objects: Optional[List[SecurableObject]] = None,
    ) -> Role:
        """Create a new role under the metalake.

        Args:
            role_name: The name of the role.
            properties: The properties of the role.
            securable_objects: The securable objects of the role.

        Returns:
            The created Role object.

        Raises:
            RoleAlreadyExistsException: If a role with the same name already exists.
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().create_role(role_name, properties, securable_objects)

    def get_role(self, role_name: str) -> Role:
        """Get a role by name from the metalake.

        Args:
            role_name: The name of the role.

        Returns:
            The Role object.

        Raises:
            NoSuchRoleException: If the role does not exist.
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().get_role(role_name)

    def delete_role(self, role_name: str) -> bool:
        """Delete a role from the metalake.

        Args:
            role_name: The name of the role.

        Returns:
            True if the role was deleted, False otherwise.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().delete_role(role_name)

    def list_role_names(self) -> list[str]:
        """List all role names under the metalake.

        Returns:
            A list of role name strings.

        Raises:
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().list_role_names()

    # Grant/Revoke operations

    def grant_roles_to_user(self, role_names: List[str], user_name: str) -> User:
        """Grant roles to a user.

        Args:
            role_names: The names of the roles to grant.
            user_name: The name of the user.

        Returns:
            The updated User object.

        Raises:
            NoSuchRoleException: If the role does not exist.
            NoSuchUserException: If the user does not exist.
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().grant_roles_to_user(role_names, user_name)

    def revoke_roles_from_user(self, role_names: List[str], user_name: str) -> User:
        """Revoke roles from a user.

        Args:
            role_names: The names of the roles to revoke.
            user_name: The name of the user.

        Returns:
            The updated User object.

        Raises:
            NoSuchRoleException: If the role does not exist.
            NoSuchUserException: If the user does not exist.
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().revoke_roles_from_user(role_names, user_name)

    def grant_roles_to_group(self, role_names: List[str], group_name: str) -> Group:
        """Grant roles to a group.

        Args:
            role_names: The names of the roles to grant.
            group_name: The name of the group.

        Returns:
            The updated Group object.

        Raises:
            NoSuchRoleException: If the role does not exist.
            NoSuchGroupException: If the group does not exist.
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().grant_roles_to_group(role_names, group_name)

    def revoke_roles_from_group(self, role_names: List[str], group_name: str) -> Group:
        """Revoke roles from a group.

        Args:
            role_names: The names of the roles to revoke.
            group_name: The name of the group.

        Returns:
            The updated Group object.

        Raises:
            NoSuchRoleException: If the role does not exist.
            NoSuchGroupException: If the group does not exist.
            NoSuchMetalakeException: If the metalake does not exist.
        """
        return self.get_metalake().revoke_roles_from_group(role_names, group_name)

    def grant_privileges_to_role(
        self,
        role_name: str,
        securable_object: SecurableObject,
        privileges: List[Privilege],
    ) -> Role:
        """Grant privileges to a role on a securable object.

        Args:
            role_name: The name of the role.
            securable_object: The securable object.
            privileges: The privileges to grant.

        Returns:
            The updated Role object.

        Raises:
            NoSuchRoleException: If the role does not exist.
            NoSuchMetalakeException: If the metalake does not exist.
            NoSuchMetadataObjectException: If the securable object does not exist.
            IllegalPrivilegeException: If a privilege is invalid.
        """
        return self.get_metalake().grant_privileges_to_role(
            role_name, securable_object, privileges
        )

    def revoke_privileges_from_role(
        self,
        role_name: str,
        securable_object: SecurableObject,
        privileges: List[Privilege],
    ) -> Role:
        """Revoke privileges from a role on a securable object.

        Args:
            role_name: The name of the role.
            securable_object: The securable object.
            privileges: The privileges to revoke.

        Returns:
            The updated Role object.

        Raises:
            NoSuchRoleException: If the role does not exist.
            NoSuchMetalakeException: If the metalake does not exist.
            NoSuchMetadataObjectException: If the securable object does not exist.
            IllegalPrivilegeException: If a privilege is invalid.
        """
        return self.get_metalake().revoke_privileges_from_role(
            role_name, securable_object, privileges
        )
