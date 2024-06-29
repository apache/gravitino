"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from typing import Optional
from dataclasses import dataclass


@dataclass
class OAuth2ClientCredentialRequest:

    grant_type: str
    client_id: Optional[str]
    client_secret: str
    scope: str

    def to_dict(self, **kwarg):
        return {k: v for k, v in self.__dict__.items() if v is not None}
