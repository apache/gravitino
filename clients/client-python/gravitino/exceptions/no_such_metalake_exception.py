"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from gravitino.exceptions.not_found_exception import NotFoundException


class NoSuchMetalakeException(NotFoundException):
    """An exception thrown when a metalake is not found."""

    def __init__(self, message, *args):
        super().__init__(message, *args)
