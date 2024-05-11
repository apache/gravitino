"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""


class GravitinoRuntimeException(RuntimeError):
    """Base class for all Gravitino runtime exceptions."""

    def __init__(self, message, *args):
        super().__init__(message.format(*args))
