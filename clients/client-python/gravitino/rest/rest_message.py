"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from abc import ABC, abstractmethod

from dataclasses_json import DataClassJsonMixin


class RESTMessage(DataClassJsonMixin, ABC):
    """
    Interface for REST messages.

    REST messages are objects that are sent to and received from REST endpoints. They are
    typically used to represent the request and response bodies of REST API calls.
    """

    @abstractmethod
    def validate(self):
        """
        Ensures that a constructed instance of a REST message is valid according to the REST spec.

        This is needed when parsing data that comes from external sources and the object might have
        been constructed without all the required fields present.

        Raises:
            IllegalArgumentException: If the message is not valid.
        """
        pass


class IllegalArgumentException(Exception):
    """Exception raised if a REST message is not valid according to the REST spec."""

    pass


class RESTRequest(RESTMessage, ABC):
    """Interface to mark a REST request."""


class RESTResponse(RESTMessage, ABC):
    """Interface to mark a REST response"""
