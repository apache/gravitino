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
import re
from typing import Dict

from gravitino.client.gravitino_client import GravitinoClient
from gravitino.auth.default_oauth2_token_provider import DefaultOAuth2TokenProvider
from gravitino.auth.oauth2_token_provider import OAuth2TokenProvider
from gravitino.auth.simple_auth_provider import SimpleAuthProvider
from gravitino.filesystem.gvfs_config import GVFSConfig
from gravitino.name_identifier import NameIdentifier
from gravitino.exceptions.base import GravitinoRuntimeException

_identifier_pattern = re.compile(
    "^(?:gvfs://)?fileset/([^/]+)/([^/]+)/([^/]+)(?:/[^/]+)*/?$"
)


def _check_auth_config(auth_type: str, config_key: str, config_value: str):
    """Check if the config value is null.
    :param auth_type: The auth type
    :param config_key: The config key
    :param config_value: The config value
    """
    if config_value is None:
        raise GravitinoRuntimeException(
            f"{config_key} should not be null"
            f" if {GVFSConfig.AUTH_TYPE} is set to {auth_type}."
        )


def create_client(
    options: Dict[str, str],
    server_uri: str,
    metalake_name: str,
    request_headers: dict = None,
    client_config: dict = None,
):
    """Create the Gravitino client.
    :param options: The options
    :param server_uri: The server URI
    :param metalake_name: The metalake name
    :param request_headers: The request headers
    :param client_config: The client config
    :return The Gravitino client
    """
    auth_type = (
        GVFSConfig.SIMPLE_AUTH_TYPE
        if options is None
        else options.get(GVFSConfig.AUTH_TYPE, GVFSConfig.SIMPLE_AUTH_TYPE)
    )

    if auth_type == GVFSConfig.SIMPLE_AUTH_TYPE:
        return GravitinoClient(
            uri=server_uri,
            metalake_name=metalake_name,
            auth_data_provider=SimpleAuthProvider(),
            request_headers=request_headers,
            client_config=client_config,
        )

    if auth_type == GVFSConfig.OAUTH2_AUTH_TYPE:
        oauth2_server_uri = options.get(GVFSConfig.OAUTH2_SERVER_URI)
        _check_auth_config(auth_type, GVFSConfig.OAUTH2_SERVER_URI, oauth2_server_uri)

        oauth2_credential = options.get(GVFSConfig.OAUTH2_CREDENTIAL)
        _check_auth_config(auth_type, GVFSConfig.OAUTH2_CREDENTIAL, oauth2_credential)

        oauth2_path = options.get(GVFSConfig.OAUTH2_PATH)
        _check_auth_config(auth_type, GVFSConfig.OAUTH2_PATH, oauth2_path)

        oauth2_scope = options.get(GVFSConfig.OAUTH2_SCOPE)
        _check_auth_config(auth_type, GVFSConfig.OAUTH2_SCOPE, oauth2_scope)

        oauth2_token_provider: OAuth2TokenProvider = DefaultOAuth2TokenProvider(
            oauth2_server_uri, oauth2_credential, oauth2_path, oauth2_scope
        )

        return GravitinoClient(
            uri=server_uri,
            metalake_name=metalake_name,
            auth_data_provider=oauth2_token_provider,
            request_headers=request_headers,
            client_config=client_config,
        )

    raise GravitinoRuntimeException(
        f"Authentication type {auth_type} is not supported."
    )


def extract_identifier(metalake_name: str, path: str):
    """Extract the fileset identifier from the path.
    :param metalake_name: The metalake name of the fileset.
    :param path: The virtual fileset path, format is
    [gvfs://fileset]/{fileset_catalog}/{fileset_schema}/{fileset_name}[/sub_path]
    :return The fileset identifier
    """
    if not metalake_name or metalake_name.isspace():
        raise ValueError("Virtual path cannot be null or empty.")

    if not path or (isinstance(path, str) and path.isspace()):
        raise GravitinoRuntimeException(
            "path which need be extracted cannot be null or empty."
        )

    match = _identifier_pattern.match(str(path))
    if match and len(match.groups()) == 3:
        return NameIdentifier.of(
            metalake_name, match.group(1), match.group(2), match.group(3)
        )
    raise GravitinoRuntimeException(
        f"path: `{path}` doesn't contains valid identifier."
    )


def get_sub_path_from_virtual_path(identifier: NameIdentifier, virtual_path: str):
    """Get the sub path from the virtual path.
    :param identifier: The identifier of the fileset.
    :param virtual_path: The virtual fileset path, format is
    fileset/{fileset_catalog}/{fileset_schema}/{fileset_name}[/sub_path]
    :return The sub path.
    """
    if not virtual_path or virtual_path.isspace():
        raise ValueError("Virtual path cannot be null or empty.")

    gvfs_path_prefix = "fileset/"
    if not virtual_path.startswith(gvfs_path_prefix):
        raise ValueError(f"Virtual path should start with '{gvfs_path_prefix}'")

    prefix = to_gvfs_path_prefix(identifier)
    if not virtual_path.startswith(prefix):
        raise ValueError(
            f"Virtual path '{virtual_path}' doesn't match fileset identifier '{identifier}'"
        )

    return virtual_path[len(prefix) :]


def to_gvfs_path_prefix(identifier: NameIdentifier):
    """Convert the fileset identifier to the virtual path prefix.
    :param identifier: The name identifier of the fileset.
    :return The virtual path prefix, format is: fileset/{fileset_catalog}/{fileset_schema}/{fileset_name}
    """
    if len(identifier.namespace().levels()) != 3:
        raise ValueError(
            f"The namespace of the identifier should have 3 levels, but got {len(identifier.namespace().levels())}"
        )

    if not identifier.name():
        raise ValueError("The identifier name should not be null or empty.")

    return (
        f"fileset/{identifier.namespace().level(1)}"
        f"/{identifier.namespace().level(2)}"
        f"/{identifier.name()}"
    )
