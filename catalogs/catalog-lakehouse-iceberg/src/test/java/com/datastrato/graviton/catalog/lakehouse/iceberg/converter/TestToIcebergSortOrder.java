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
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test class for {@link ToIcebergSortOrder}. */
public class TestToIcebergSortOrder extends TestBaseConvert {

  @Test
  public void testToSortOrder() {
    SortOrder[] sortOrders = createSortOrder("col_1", "col_2", "col_3", "col_4", "col_5");
    sortOrders =
        ArrayUtils.add(sortOrders, createFunctionSortOrder(Transforms.NAME_OF_DAY, "col_6"));
    sortOrders =
        ArrayUtils.add(sortOrders, createFunctionSortOrder(Transforms.NAME_OF_HOUR, "col_7"));
    sortOrders =
        ArrayUtils.add(sortOrders, createFunctionSortOrder(Transforms.NAME_OF_MONTH, "col_8"));
    sortOrders =
        ArrayUtils.add(sortOrders, createFunctionSortOrder(Transforms.NAME_OF_YEAR, "col_8"));

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
                    sortOrder -> {
                      if (sortOrder.getTransform() instanceof Transforms.NamedReference) {
                        return ((Transforms.NamedReference) sortOrder.getTransform()).value()[0];
                      } else if (sortOrder.getTransform() instanceof Transforms.FunctionTrans) {
                        return ((Transforms.NamedReference) sortOrder.getTransform().arguments()[0])
                            .value()[0];
                      } else {
                        throw new UnsupportedOperationException("Unsupported sort type.");
                      }
                    },
                    v -> v));
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
          sortOrder.getDirection() == SortOrder.Direction.ASC
              ? SortDirection.ASC
              : SortDirection.DESC,
          sortField.direction());
      Assertions.assertEquals(
          sortOrder.getNullOrdering() == SortOrder.NullOrdering.FIRST
              ? NullOrder.NULLS_FIRST
              : NullOrder.NULLS_LAST,
          sortField.nullOrder());
    }
  }
}
