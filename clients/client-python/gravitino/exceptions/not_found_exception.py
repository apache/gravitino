"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from gravitino.exceptions.gravitino_runtime_exception import GravitinoRuntimeException


class NotFoundException(GravitinoRuntimeException):
    """Base class for all exceptions thrown when a resource is not found."""

    def __init__(self, message, *args):
        super().__init__(message, *args)
