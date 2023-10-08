/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg.converter;

import com.datastrato.graviton.rel.SortOrder;
import com.datastrato.graviton.rel.transforms.Transforms;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test class for {@link FromIcebergSortOrder}. */
public class TestFromIcebergSortOrder extends TestBaseConvert {

  @Test
  public void testFromSortOrder() {
    SortOrder[] sortOrders = createSortOrder("col_1", "col_2", "col_3");
    Types.NestedField[] nestedFields = createNestedField("col_1", "col_2", "col_3");
    Schema schema = new Schema(nestedFields);
    org.apache.iceberg.SortOrder icebergSortOrder =
        ToIcebergSortOrder.toSortOrder(schema, sortOrders);

    List<SortField> sortFields = icebergSortOrder.fields();
    Assertions.assertEquals(sortOrders.length, sortFields.size());

    Map<String, SortOrder> sortOrderByName =
        Arrays.stream(sortOrders)
            .collect(
                Collectors.toMap(
                    o -> ((Transforms.NamedReference) o.getTransform()).value()[0], v -> v));
    Map<Integer, String> idToName = schema.idToName();
    for (SortField sortField : sortFields) {
      Assertions.assertTrue(idToName.containsKey(sortField.sourceId()));
      String sortOrderName = idToName.get(sortField.sourceId());
      Assertions.assertTrue(sortOrderByName.containsKey(sortOrderName));
      SortOrder sortOrder = sortOrderByName.get(sortOrderName);
      Assertions.assertEquals(
          sortField.direction() == SortDirection.ASC
              ? SortOrder.Direction.ASC
              : SortOrder.Direction.DESC,
          sortOrder.getDirection());
      Assertions.assertEquals(
          sortField.nullOrder() == NullOrder.NULLS_FIRST
              ? SortOrder.NullOrdering.FIRST
              : SortOrder.NullOrdering.LAST,
          sortOrder.getNullOrdering());
    }
  }
}
