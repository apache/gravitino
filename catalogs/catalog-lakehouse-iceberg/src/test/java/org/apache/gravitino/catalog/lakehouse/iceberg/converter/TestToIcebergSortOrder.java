/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.lakehouse.iceberg.converter;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortField;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test class for {@link ToIcebergSortOrder}. */
public class TestToIcebergSortOrder extends TestBaseConvert {

  @Test
  public void testToSortOrder() {
    SortOrder[] sortOrders = createSortOrder("col_1", "col_2", "col_3", "col_4", "col_5");
    sortOrders = ArrayUtils.add(sortOrders, createSortOrder("day", "col_6"));
    sortOrders = ArrayUtils.add(sortOrders, createSortOrder("hour", "col_7"));
    sortOrders = ArrayUtils.add(sortOrders, createSortOrder("month", "col_8"));
    sortOrders = ArrayUtils.add(sortOrders, createSortOrder("year", "col_9"));
    sortOrders = ArrayUtils.add(sortOrders, createSortOrder("bucket", 10, "col_10"));
    sortOrders = ArrayUtils.add(sortOrders, createSortOrder("truncate", 2, "col_11"));

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
    nestedFields =
        ArrayUtils.add(nestedFields, createNestedField(10, "col_10", Types.IntegerType.get()));
    nestedFields =
        ArrayUtils.add(nestedFields, createNestedField(11, "col_11", Types.StringType.get()));
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
      String icebergSortOrderString = getIcebergTransfromString(sortField, schema);
      String gravitinoSortOrderString =
          getGravitinoSortOrderExpressionString(sortOrder.expression());
      Assertions.assertEquals(icebergSortOrderString, gravitinoSortOrderString);
    }
  }
}
