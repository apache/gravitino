"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""


class IllegalNamespaceException(Exception):
    """An exception thrown when a namespace is invalid."""

    def __init__(self, message=None):
        if message:
            super().__init__(message)
        else:
            super().__init__()
