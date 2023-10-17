/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import com.datastrato.gravitino.rel.SortOrder;
import com.datastrato.gravitino.rel.transforms.Transforms;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test class for {@link FromIcebergSortOrder}. */
public class TestFromIcebergSortOrder extends TestBaseConvert {

  @Test
  public void testFromSortOrder() {
    Types.NestedField[] nestedFields = createNestedField("col_1", "col_2", "col_3");
    nestedFields =
        ArrayUtils.add(nestedFields, createNestedField(4, "col_4", Types.DateType.get()));
    nestedFields =
        ArrayUtils.add(nestedFields, createNestedField(5, "col_5", Types.DateType.get()));
    nestedFields =
        ArrayUtils.add(
            nestedFields, createNestedField(6, "col_6", Types.TimestampType.withoutZone()));
    nestedFields =
        ArrayUtils.add(nestedFields, createNestedField(7, "col_7", Types.IntegerType.get()));
    nestedFields =
        ArrayUtils.add(nestedFields, createNestedField(8, "col_8", Types.IntegerType.get()));

    Schema schema = new Schema(nestedFields);
    org.apache.iceberg.SortOrder.Builder sortOrderBuilder =
        org.apache.iceberg.SortOrder.builderFor(schema);
    sortOrderBuilder.sortBy("col_2", SortDirection.DESC, NullOrder.NULLS_FIRST);
    sortOrderBuilder.sortBy("col_3", SortDirection.ASC, NullOrder.NULLS_LAST);
    sortOrderBuilder.sortBy(Expressions.year("col_4"), SortDirection.DESC, NullOrder.NULLS_LAST);
    sortOrderBuilder.sortBy(Expressions.day("col_5"), SortDirection.ASC, NullOrder.NULLS_FIRST);
    sortOrderBuilder.sortBy(Expressions.hour("col_6"), SortDirection.ASC, NullOrder.NULLS_LAST);
    sortOrderBuilder.sortBy(
        Expressions.bucket("col_7", 5), SortDirection.DESC, NullOrder.NULLS_FIRST);
    sortOrderBuilder.sortBy(
        Expressions.truncate("col_8", 8), SortDirection.ASC, NullOrder.NULLS_FIRST);
    org.apache.iceberg.SortOrder icebergSortOrder = sortOrderBuilder.build();
    SortOrder[] sortOrders = FromIcebergSortOrder.fromSortOrder(sortOrderBuilder.build());
    Assertions.assertEquals(sortOrders.length, 7);

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
                      }
                      throw new RuntimeException("Unsupported transform type");
                    },
                    v -> v));
    Map<Integer, String> idToName = schema.idToName();
    List<SortField> sortFields = icebergSortOrder.fields();
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
