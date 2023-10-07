/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg.converter;

import com.datastrato.graviton.rel.SortOrder;
import com.datastrato.graviton.rel.transforms.Transform;
import com.datastrato.graviton.rel.transforms.Transforms;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;

/** Implement iceberg sort order converter to graviton sort order. */
public class ToIcebergSortOrder {

  /**
   * Convert graviton's order to iceberg's.
   *
   * @param schema
   * @param sortOrders
   * @return
   */
  public static org.apache.iceberg.SortOrder toSortOrder(Schema schema, SortOrder[] sortOrders) {
    if (ArrayUtils.isEmpty(sortOrders)) {
      return null;
    }
    org.apache.iceberg.SortOrder icebergSortOrder;
    org.apache.iceberg.SortOrder.Builder sortOrderBuilder =
        org.apache.iceberg.SortOrder.builderFor(schema);
    for (SortOrder sortOrder : sortOrders) {
      Transform transform = sortOrder.getTransform();
      if (transform instanceof Transforms.NamedReference) {
        String[] fieldName = ((Transforms.NamedReference) transform).value();
        for (String name : fieldName) {
          sortOrderBuilder.sortBy(
              name,
              sortOrder.getDirection() == SortOrder.Direction.ASC
                  ? SortDirection.ASC
                  : SortDirection.DESC,
              sortOrder.getNullOrdering() == SortOrder.NullOrdering.FIRST
                  ? NullOrder.NULLS_FIRST
                  : NullOrder.NULLS_LAST);
        }
      } else if (transform instanceof Transforms.FunctionTrans) {
        Preconditions.checkArgument(
            transform.arguments().length == 1,
            "Iceberg sort order does not support nested field",
            transform);
        String colName =
            Arrays.stream(transform.arguments())
                .map(t -> ((Transforms.NamedReference) t).value()[0])
                .collect(Collectors.joining());
        sortOrderBuilder.sortBy(
            colName,
            sortOrder.getDirection() == SortOrder.Direction.ASC
                ? SortDirection.ASC
                : SortDirection.DESC,
            sortOrder.getNullOrdering() == SortOrder.NullOrdering.FIRST
                ? NullOrder.NULLS_FIRST
                : NullOrder.NULLS_LAST);
      } else {
        throw new UnsupportedOperationException("Transform is not supported: " + transform.name());
      }
    }
    icebergSortOrder = sortOrderBuilder.build();
    return icebergSortOrder;
  }
}
