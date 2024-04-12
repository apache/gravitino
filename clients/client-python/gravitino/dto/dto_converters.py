"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from gravitino.dto.requests.metalake_update_request import MetalakeUpdateRequest
from gravitino.meta_change import MetalakeChange


class DTOConverters:
    """Utility class for converting between DTOs and domain objects."""

    @staticmethod
    def to_metalake_update_request(change: MetalakeChange) -> object:
        # Assuming MetalakeUpdateRequest has similar nested class structure for requests
        if isinstance(change, MetalakeChange.RenameMetalake):
            return MetalakeUpdateRequest.RenameMetalakeRequest(change.newName)
        elif isinstance(change, MetalakeChange.UpdateMetalakeComment):
            return MetalakeUpdateRequest.UpdateMetalakeCommentRequest(change.newComment)
        elif isinstance(change, MetalakeChange.SetProperty):
            return MetalakeUpdateRequest.SetMetalakePropertyRequest(change.property, change.value)
        elif isinstance(change, MetalakeChange.RemoveProperty):
            return MetalakeUpdateRequest.RemoveMetalakePropertyRequest(change.property)
        else:
            raise ValueError(f"Unknown change type: {type(change).__name__}")
