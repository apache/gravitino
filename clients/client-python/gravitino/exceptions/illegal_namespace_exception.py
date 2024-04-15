"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""


class IllegalNamespaceException(Exception):
    """An exception thrown when a namespace is invalid."""

    def __init__(self, message=None, *args):
        if message:
            super().__init__(message.format(*args))
        else:
            super().__init__()
