"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import urllib.parse


def encode_string(to_encode: str):

    assert to_encode is not None, "Invalid string to encode: None"

    return urllib.parse.quote(to_encode)
