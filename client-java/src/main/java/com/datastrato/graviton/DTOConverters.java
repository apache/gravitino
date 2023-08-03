/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton;

import com.datastrato.graviton.client.RESTClient;
import com.datastrato.graviton.dto.AuditDTO;
import com.datastrato.graviton.dto.MetalakeDTO;
import com.datastrato.graviton.dto.requests.MetalakeUpdateRequest;

/** Utility class for converting between DTO objects and GravitonMetaLake objects. */
class DTOConverters {
  private DTOConverters() {}

  /**
   * Converts a MetalakeDTO object to a GravitonMetaLake object.
   *
   * @param metalake The Metalake DTO to be converted.
   * @param client The REST client associated with the GravitonMetaLake.
   * @return A GravitonMetaLake object representing the converted MetalakeDTO.
   */
  static GravitonMetaLake toMetaLake(MetalakeDTO metalake, RESTClient client) {
    return new GravitonMetaLake.Builder()
        .withName(metalake.name())
        .withComment(metalake.comment())
        .withProperties(metalake.properties())
        .withAudit((AuditDTO) metalake.auditInfo())
        .withRestClient(client)
        .build();
  }

  /**
   * Converts a MetalakeChange object to a MetalakeUpdateRequest object.
   *
   * @param change The Metalake change to be converted.
   * @return A MetalakeUpdateRequest object representing the converted Metalake change.
   * @throws IllegalArgumentException If the provided Metalake change is of an unknown type.
   */
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
