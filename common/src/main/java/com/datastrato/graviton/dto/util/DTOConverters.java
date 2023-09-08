/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.dto.util;

import com.datastrato.graviton.Distribution;
import com.datastrato.graviton.SortOrder;
import com.datastrato.graviton.dto.rel.DistributionDTO;
import com.datastrato.graviton.dto.rel.DistributionDTO.Builder;
import com.datastrato.graviton.dto.rel.DistributionDTO.DistributionMethod;
import com.datastrato.graviton.dto.rel.PartitionUtils;
import com.datastrato.graviton.dto.rel.SortOrderDTO;
import com.datastrato.graviton.dto.rel.SortOrderDTO.Direction;
import com.datastrato.graviton.rel.transforms.Transform;
import java.util.Arrays;
import java.util.stream.Collectors;

public class DTOConverters {

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
