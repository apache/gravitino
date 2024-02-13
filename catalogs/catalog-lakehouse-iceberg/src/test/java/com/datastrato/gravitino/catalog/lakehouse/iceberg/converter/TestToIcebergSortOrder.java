/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import com.datastrato.gravitino.rel.expressions.sorts.NullOrdering;
import com.datastrato.gravitino.rel.expressions.sorts.SortDirection;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortField;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test class for {@link ToIcebergSortOrder}. */
public class TestToIcebergSortOrder extends TestBaseConvert {

  @Test
  void testToSortOrder() {
    SortOrder[] sortOrders = createSortOrder("col_1", "col_2", "col_3", "col_4", "col_5");
    sortOrders = ArrayUtils.add(sortOrders, createFunctionSortOrder("day", "col_6"));
    sortOrders = ArrayUtils.add(sortOrders, createFunctionSortOrder("hour", "col_7"));
    sortOrders = ArrayUtils.add(sortOrders, createFunctionSortOrder("month", "col_8"));
    sortOrders = ArrayUtils.add(sortOrders, createFunctionSortOrder("year", "col_9"));

    Types.NestedField[] nestedFields =
        createNestedField("col_1", "col_2", "col_3", "col_4", "col_5");
    nestedFields =
        ArrayUtils.add(nestedFields, createNestedField(6, "col_6", Types.DateType.get()));
    nestedFields =
        ArrayUtils.add(nestedFields, createNestedField(7, "col_7", Types.TimestampType.withZone()));
    nestedFields =
        ArrayUtils.add(nestedFields, createNestedField(8, "col_8", Types.DateType.get()));
    nestedFields =
        ArrayUtils.add(nestedFields, createNestedField(9, "col_9", Types.DateType.get()));
    Schema schema = new Schema(nestedFields);
    org.apache.iceberg.SortOrder icebergSortOrder =
        ToIcebergSortOrder.toSortOrder(schema, sortOrders);

    List<SortField> sortFields = icebergSortOrder.fields();
    Assertions.assertEquals(sortOrders.length, sortFields.size());

    Map<Integer, String> idToName = schema.idToName();
    Map<String, SortOrder> sortOrderByName =
        Arrays.stream(sortOrders)
            .collect(
                Collectors.toMap(
                    sortOrder -> sortOrder.expression().references()[0].fieldName()[0], v -> v));
    for (SortField sortField : sortFields) {
      Assertions.assertTrue(idToName.containsKey(sortField.sourceId()));
      String colName = idToName.get(sortField.sourceId());
      Assertions.assertTrue(sortOrderByName.containsKey(colName));
      SortOrder sortOrder = sortOrderByName.get(colName);
      if (colName.equals("col_6")
          || colName.equals("col_7")
          || colName.equals("col_8")
          || colName.equals("col_9")) {
        Assertions.assertFalse(sortField.transform().isIdentity());
      } else {
        Assertions.assertTrue(sortField.transform().isIdentity());
      }
      Assertions.assertEquals(
          sortOrder.direction() == SortDirection.ASCENDING
              ? org.apache.iceberg.SortDirection.ASC
              : org.apache.iceberg.SortDirection.DESC,
          sortField.direction());
      Assertions.assertEquals(
          sortOrder.nullOrdering() == NullOrdering.NULLS_FIRST
              ? NullOrder.NULLS_FIRST
              : NullOrder.NULLS_LAST,
          sortField.nullOrder());
    }
  }
}
