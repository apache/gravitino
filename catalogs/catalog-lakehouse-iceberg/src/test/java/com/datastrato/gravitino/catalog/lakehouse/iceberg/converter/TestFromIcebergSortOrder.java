/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import com.datastrato.gravitino.rel.expressions.FunctionExpression;
import com.datastrato.gravitino.rel.expressions.NamedReference;
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
    sortOrderBuilder.sortBy("col_2", org.apache.iceberg.SortDirection.DESC, NullOrder.NULLS_FIRST);
    sortOrderBuilder.sortBy("col_3", org.apache.iceberg.SortDirection.ASC, NullOrder.NULLS_LAST);
    sortOrderBuilder.sortBy(
        Expressions.year("col_4"), org.apache.iceberg.SortDirection.DESC, NullOrder.NULLS_LAST);
    sortOrderBuilder.sortBy(
        Expressions.day("col_5"), org.apache.iceberg.SortDirection.ASC, NullOrder.NULLS_FIRST);
    sortOrderBuilder.sortBy(
        Expressions.hour("col_6"), org.apache.iceberg.SortDirection.ASC, NullOrder.NULLS_LAST);
    sortOrderBuilder.sortBy(
        Expressions.bucket("col_7", 5),
        org.apache.iceberg.SortDirection.DESC,
        NullOrder.NULLS_FIRST);
    sortOrderBuilder.sortBy(
        Expressions.truncate("col_8", 8),
        org.apache.iceberg.SortDirection.ASC,
        NullOrder.NULLS_FIRST);
    org.apache.iceberg.SortOrder icebergSortOrder = sortOrderBuilder.build();
    SortOrder[] sortOrders = FromIcebergSortOrder.fromSortOrder(sortOrderBuilder.build());
    Assertions.assertEquals(7, sortOrders.length);

    Map<String, SortOrder> sortOrderByName =
        Arrays.stream(sortOrders)
            .collect(
                Collectors.toMap(
                    sortOrder -> {
                      if (sortOrder.expression() instanceof NamedReference.FieldReference) {
                        return ((NamedReference.FieldReference) sortOrder.expression())
                            .fieldName()[0];
                      } else if (sortOrder.expression() instanceof FunctionExpression) {
                        return ((NamedReference.FieldReference)
                                ((FunctionExpression) sortOrder.expression()).arguments()[0])
                            .fieldName()[0];
                      }
                      throw new RuntimeException("Unsupported sort expression type");
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
          sortField.direction() == org.apache.iceberg.SortDirection.ASC
              ? SortDirection.ASCENDING
              : SortDirection.DESCENDING,
          sortOrder.direction());
      Assertions.assertEquals(
          sortField.nullOrder() == NullOrder.NULLS_FIRST
              ? NullOrdering.NULLS_FIRST
              : NullOrdering.NULLS_LAST,
          sortOrder.nullOrdering());
    }
  }
}
