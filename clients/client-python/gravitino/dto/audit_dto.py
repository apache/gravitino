"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import DataClassJsonMixin, config


@dataclass
class AuditDTO(DataClassJsonMixin):
    """Data transfer object representing audit information."""

    creator: Optional[str]
    """The creator of the audit."""

    create_time: Optional[str] = field(metadata=config(field_name='createTime'))  # TODO: Can't deserialized datetime from JSON
    """The create time of the audit."""

    last_modifier: Optional[str] = field(metadata=config(field_name='lastModifier'))
    """The last modifier of the audit."""

    last_modified_time: Optional[str] = field(
        metadata=config(field_name='lastModifiedTime'))  # TODO: Can't deserialized datetime from JSON
    """The last modified time of the audit."""

    def __init__(self, creator: str = None, create_time: str = None, last_modifier: str = None,
                 last_modified_time: str = None):
        self.creator: str = creator
        self.create_time: str = create_time
        self.last_modifier: str = last_modifier
        self.last_modified_time: str = last_modified_time
