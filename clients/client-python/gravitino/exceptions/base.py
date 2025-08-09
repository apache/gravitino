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


class GravitinoRuntimeException(RuntimeError):
    """Base class for all Gravitino runtime exceptions."""

    def __init__(self, message, *args):
        super().__init__(message % args)


class RESTException(RuntimeError):
    """An exception thrown when a REST request fails."""

    def __init__(self, message, *args):
        super().__init__(message % args)


class IllegalArgumentException(ValueError):
    """Base class for all exceptions thrown when arguments are invalid."""

    def __init__(self, message, *args):
        super().__init__(message % args)


class IllegalNameIdentifierException(IllegalArgumentException):
    """An exception thrown when a name identifier is invalid."""


class IllegalNamespaceException(IllegalArgumentException):
    """An exception thrown when a namespace is invalid."""


class InternalError(GravitinoRuntimeException):
    """Base class for all exceptions thrown internally"""


class NotFoundException(GravitinoRuntimeException):
    """Base class for all exceptions thrown when a resource is not found."""


class NoSuchSchemaException(NotFoundException):
    """An exception thrown when a schema is not found."""


class NoSuchFilesetException(NotFoundException):
    """Exception thrown when a file with specified name is not existed."""


class NoSuchLocationNameException(NotFoundException):
    """Exception thrown when a fileset location name is not found."""


class NoSuchCredentialException(NotFoundException):
    """Exception thrown when a credential with specified credential type is not existed."""


class NoSuchMetalakeException(NotFoundException):
    """An exception thrown when a metalake is not found."""


class NoSuchCatalogException(NotFoundException):
    """An exception thrown when a catalog is not found."""


class NoSuchModelException(NotFoundException):
    """An exception thrown when a model is not found."""


class NoSuchModelVersionException(NotFoundException):
    """An exception thrown when a model version is not found."""


class NoSuchModelVersionURINameException(NotFoundException):
    """An exception thrown when a URI name of a model version is not found."""


class AlreadyExistsException(GravitinoRuntimeException):
    """Base exception thrown when an entity or resource already exists."""


class MetalakeAlreadyExistsException(AlreadyExistsException):
    """An exception thrown when a metalake already exists."""


class SchemaAlreadyExistsException(AlreadyExistsException):
    """An exception thrown when a schema already exists."""


class CatalogAlreadyExistsException(AlreadyExistsException):
    """An exception thrown when a resource already exists."""


class ModelAlreadyExistsException(AlreadyExistsException):
    """An exception thrown when a model already exists."""


class ModelVersionAliasesAlreadyExistException(AlreadyExistsException):
    """An exception thrown when model version with aliases already exists."""


class NotEmptyException(GravitinoRuntimeException):
    """Base class for all exceptions thrown when a resource is not empty."""


class NonEmptySchemaException(NotEmptyException):
    """Exception thrown when a namespace is not empty."""


class InUseException(GravitinoRuntimeException):
    """Base class for all exceptions thrown when an entity is in use and cannot be deleted."""


class MetalakeInUseException(InUseException):
    """An exception thrown when a metalake is in use and cannot be deleted."""


class CatalogInUseException(InUseException):
    """An Exception thrown when a catalog is in use and cannot be deleted."""


class NotInUseException(GravitinoRuntimeException):
    """Base class for all exceptions thrown when an entity is not in use."""


class MetalakeNotInUseException(NotInUseException):
    """An exception thrown when operating on not in use metalake."""


class CatalogNotInUseException(NotInUseException):
    """An exception thrown when operating on not in use catalog."""


class UnsupportedOperationException(GravitinoRuntimeException):
    """Base class for all exceptions thrown when an operation is unsupported"""


class UnknownError(RuntimeError):
    """An exception thrown when other unknown exception is thrown"""


class ConnectionFailedException(GravitinoRuntimeException):
    """An exception thrown when connect to catalog failed."""


class UnauthorizedException(GravitinoRuntimeException):
    """An exception thrown when a user is not authorized to perform an action."""


class BadRequestException(GravitinoRuntimeException):
    """An exception thrown when the request is invalid."""


class IllegalStateException(GravitinoRuntimeException):
    """An exception thrown when the state is invalid."""


class NoSuchTagException(NotFoundException):
    """An exception thrown when a tag with specified name is not existed."""


class TagAlreadyExistsException(AlreadyExistsException):
    """An exception thrown when a tag with specified name already associated to a metadata object."""


class JobTemplateAlreadyExistsException(AlreadyExistsException):
    """An exception thrown when a job template with specified name already exists."""


class NoSuchJobTemplateException(NotFoundException):
    """An exception thrown when a job template with specified name is not found."""


class NoSuchJobException(NotFoundException):
    """An exception thrown when a job with specified name is not found."""


class ForbiddenException(GravitinoRuntimeException):
    """An exception thrown when a user is forbidden to perform an action."""
