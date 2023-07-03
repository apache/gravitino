package com.datastrato.graviton;

import com.datastrato.graviton.client.RESTClient;
import com.datastrato.graviton.dto.AuditDTO;
import com.datastrato.graviton.dto.MetalakeDTO;
import com.datastrato.graviton.dto.requests.MetalakeUpdateRequest;

class DTOConverters {
  private DTOConverters() {}

  static GravitonMetaLake toMetaLake(MetalakeDTO metalake, RESTClient client) {
    return new GravitonMetaLake.Builder()
        .withName(metalake.name())
        .withComment(metalake.comment())
        .withProperties(metalake.properties())
        .withAudit((AuditDTO) metalake.auditInfo())
        .withRestClient(client)
        .build();
  }

  static MetalakeUpdateRequest toMetalakeUpdateRequest(MetalakeChange change) {
    if (change instanceof MetalakeChange.RenameMetalake) {
      return new MetalakeUpdateRequest.RenameMetalakeRequest(
          ((MetalakeChange.RenameMetalake) change).getNewName());

    } else if (change instanceof MetalakeChange.UpdateMetalakeComment) {
      return new MetalakeUpdateRequest.UpdateMetalakeCommentRequest(
          ((MetalakeChange.UpdateMetalakeComment) change).getNewComment());

    } else if (change instanceof MetalakeChange.SetProperty) {
      return new MetalakeUpdateRequest.SetMetalakePropertyRequest(
          ((MetalakeChange.SetProperty) change).getProperty(),
          ((MetalakeChange.SetProperty) change).getValue());

    } else if (change instanceof MetalakeChange.RemoveProperty) {
      return new MetalakeUpdateRequest.RemoveMetalakePropertyRequest(
          ((MetalakeChange.RemoveProperty) change).getProperty());

    } else {
      throw new IllegalArgumentException(
          "Unknown change type: " + change.getClass().getSimpleName());
    }
  }
}
