"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from pydantic import BaseModel

class VersionDTO(BaseModel):
    version: str
    compile_date: str
    git_commit: str
