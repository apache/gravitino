/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.client;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.CatalogChange;
import com.datastrato.graviton.MetalakeChange;
import com.datastrato.graviton.dto.AuditDTO;
import com.datastrato.graviton.dto.CatalogDTO;
import com.datastrato.graviton.dto.MetalakeDTO;
import com.datastrato.graviton.dto.requests.CatalogUpdateRequest;
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

  static Catalog toCatalog(CatalogDTO catalog, RESTClient client) {
    switch (catalog.type()) {
      case RELATIONAL:
        return new RelationalCatalog.Builder()
            .withName(catalog.name())
            .withType(catalog.type())
            .withComment(catalog.comment())
            .withProperties(catalog.properties())
            .withAudit((AuditDTO) catalog.auditInfo())
            .withRestClient(client)
            .build();

      case FILE:
      case STREAM:
      default:
        throw new UnsupportedOperationException("Unsupported catalog type: " + catalog.type());
    }
  }

  static CatalogUpdateRequest toCatalogUpdateRequest(CatalogChange change) {
    if (change instanceof CatalogChange.RenameCatalog) {
      return new CatalogUpdateRequest.RenameCatalogRequest(
          ((CatalogChange.RenameCatalog) change).getNewName());

    } else if (change instanceof CatalogChange.UpdateCatalogComment) {
      return new CatalogUpdateRequest.UpdateCatalogCommentRequest(
          ((CatalogChange.UpdateCatalogComment) change).getNewComment());

    } else if (change instanceof CatalogChange.SetProperty) {
      return new CatalogUpdateRequest.SetCatalogPropertyRequest(
          ((CatalogChange.SetProperty) change).getProperty(),
          ((CatalogChange.SetProperty) change).getValue());

    } else if (change instanceof CatalogChange.RemoveProperty) {
      return new CatalogUpdateRequest.RemoveCatalogPropertyRequest(
          ((CatalogChange.RemoveProperty) change).getProperty());

    } else {
      throw new IllegalArgumentException(
          "Unknown change type: " + change.getClass().getSimpleName());
    }
  }
}
