"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""


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


class NoSuchMetalakeException(NotFoundException):
    """An exception thrown when a metalake is not found."""


class NoSuchCatalogException(NotFoundException):
    """An exception thrown when a catalog is not found."""


class AlreadyExistsException(GravitinoRuntimeException):
    """Base exception thrown when an entity or resource already exists."""


class MetalakeAlreadyExistsException(AlreadyExistsException):
    """An exception thrown when a metalake already exists."""


class SchemaAlreadyExistsException(AlreadyExistsException):
    """An exception thrown when a schema already exists."""


class CatalogAlreadyExistsException(AlreadyExistsException):
    """An exception thrown when a resource already exists."""


class NotEmptyException(GravitinoRuntimeException):
    """Base class for all exceptions thrown when a resource is not empty."""


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
