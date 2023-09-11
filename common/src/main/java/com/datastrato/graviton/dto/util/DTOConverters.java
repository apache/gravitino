/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.util;

import com.datastrato.graviton.Audit;
import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.Distribution;
import com.datastrato.graviton.Metalake;
import com.datastrato.graviton.SortOrder;
import com.datastrato.graviton.dto.AuditDTO;
import com.datastrato.graviton.dto.CatalogDTO;
import com.datastrato.graviton.dto.MetalakeDTO;
import com.datastrato.graviton.dto.rel.ColumnDTO;
import com.datastrato.graviton.dto.rel.DistributionDTO;
import com.datastrato.graviton.dto.rel.DistributionDTO.Builder;
import com.datastrato.graviton.dto.rel.DistributionDTO.DistributionMethod;
import com.datastrato.graviton.dto.rel.PartitionUtils;
import com.datastrato.graviton.dto.rel.SchemaDTO;
import com.datastrato.graviton.dto.rel.SortOrderDTO;
import com.datastrato.graviton.dto.rel.SortOrderDTO.Direction;
import com.datastrato.graviton.dto.rel.TableDTO;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Schema;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.transforms.Transform;
import java.util.Arrays;
import java.util.stream.Collectors;

public class DTOConverters {

  private DTOConverters() {}

  public static AuditDTO toDTO(Audit audit) {
    return AuditDTO.builder()
        .withCreator(audit.creator())
        .withCreateTime(audit.createTime())
        .withLastModifier(audit.lastModifier())
        .withLastModifiedTime(audit.lastModifiedTime())
        .build();
  }

  public static MetalakeDTO toDTO(Metalake metalake) {
    return new MetalakeDTO.Builder()
        .withName(metalake.name())
        .withComment(metalake.comment())
        .withProperties(metalake.properties())
        .withAudit(toDTO(metalake.auditInfo()))
        .build();
  }

  public static CatalogDTO toDTO(Catalog catalog) {
    return new CatalogDTO.Builder()
        .withName(catalog.name())
        .withType(catalog.type())
        .withComment(catalog.comment())
        .withProperties(catalog.properties())
        .withAudit(toDTO(catalog.auditInfo()))
        .build();
  }

  public static SchemaDTO toDTO(Schema schema) {
    return new SchemaDTO.Builder()
        .withName(schema.name())
        .withComment(schema.comment())
        .withProperties(schema.properties())
        .withAudit(toDTO(schema.auditInfo()))
        .build();
  }

  public static ColumnDTO toDTO(Column column) {
    return new ColumnDTO.Builder()
        .withName(column.name())
        .withDataType(column.dataType())
        .withComment(column.comment())
        .build();
  }

  public static TableDTO toDTO(Table table) {
    return new TableDTO.Builder()
        .withName(table.name())
        .withComment(table.comment())
        .withColumns(
            Arrays.stream(table.columns()).map(DTOConverters::toDTO).toArray(ColumnDTO[]::new))
        .withProperties(table.properties())
        .withAudit(toDTO(table.auditInfo()))
        .build();
  }

  public static DistributionDTO fromDistrition(Distribution distribution) {
    return new Builder()
        .withDistMethod(DistributionMethod.fromString(distribution.distMethod().name()))
        .withDistNum(distribution.distNum())
        .withExpressions(
            Arrays.stream(distribution.transforms())
                .map(PartitionUtils::toExpression)
                .collect(Collectors.toList()))
        .build();
  }

  public static Distribution toDTO(DistributionDTO distributionDTO) {
    return Distribution.builder()
        .distMethod(
            Distribution.DistributionMethod.valueOf(distributionDTO.getDistributionMethod().name()))
        .distNum(distributionDTO.getDistNum())
        .transforms(
            distributionDTO.getExpressions().stream()
                .map(PartitionUtils::toTransform)
                .toArray(Transform[]::new))
        .build();
  }

  public static SortOrderDTO fromSortOrder(SortOrder sortOrder) {
    return new SortOrderDTO.Builder()
        .withExpression(PartitionUtils.toExpression(sortOrder.getTransform()))
        .withDirection(Direction.fromString(sortOrder.getDirection().name()))
        .withNullOrder(
            com.datastrato.graviton.dto.rel.SortOrderDTO.NullOrder.fromString(
                sortOrder.getNullOrder().name()))
        .build();
  }

  public static SortOrder toDTO(SortOrderDTO sortOrderDTO) {
    return SortOrder.builder()
        .direction(SortOrder.Direction.valueOf(sortOrderDTO.getDirection().name()))
        .nullOrder(SortOrder.NullOrder.valueOf(sortOrderDTO.getNullOrder().name()))
        .transform(PartitionUtils.toTransform(sortOrderDTO.getExpression()))
        .build();
  }
}
