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
package org.apache.gravitino.stats;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStatisticValue {

  @Test
  public void testLiterals() {
    StatisticValues.BooleanValue booleanValue = StatisticValues.booleanValue(true);
    Assertions.assertTrue(booleanValue.value());
    Assertions.assertEquals(booleanValue.dataType(), Types.BooleanType.get());

    StatisticValues.LongValue longValue = StatisticValues.longValue(100L);
    Assertions.assertEquals(longValue.value(), 100L);
    Assertions.assertEquals(longValue.dataType(), Types.LongType.get());

    StatisticValues.DoubleValue doubleValue = StatisticValues.doubleValue(99.99);
    Assertions.assertEquals(doubleValue.value(), 99.99);
    Assertions.assertEquals(doubleValue.dataType(), Types.DoubleType.get());

    StatisticValues.StringValue stringValue = StatisticValues.stringValue("test");
    Assertions.assertEquals("test", stringValue.value());
    Assertions.assertEquals(stringValue.dataType(), Types.StringType.get());

    List<StatisticValue<String>> list =
        Lists.newArrayList(StatisticValues.stringValue("a"), StatisticValues.stringValue("b"));
    StatisticValues.ListValue<String> listValue = StatisticValues.listValue(list);
    Assertions.assertEquals(2, listValue.value().size());
    Assertions.assertEquals(listValue.dataType(), Types.ListType.nullable(Types.StringType.get()));
    Assertions.assertIterableEquals(list, listValue.value());

    Map<String, StatisticValue<?>> map = Maps.newHashMap();
    map.put("a", StatisticValues.stringValue("a"));
    map.put("b", StatisticValues.longValue(2L));
    StatisticValues.ObjectValue objectValue = StatisticValues.objectValue(map);
    Assertions.assertEquals(2, objectValue.value().size());
    Assertions.assertEquals(map.get("a"), objectValue.value().get("a"));
    Assertions.assertEquals(map.get("b"), objectValue.value().get("b"));
    Assertions.assertEquals(
        Types.StructType.of(
            Types.StructType.Field.nullableField("a", Types.StringType.get()),
            Types.StructType.Field.nullableField("b", Types.LongType.get())),
        objectValue.dataType());
  }

  @Test
  public void testObjectMapOrder() {
    Map<String, StatisticValue<?>> reverseSortedMap =
        new TreeMap<String, StatisticValue<?>>(Comparator.reverseOrder());
    reverseSortedMap.put("a", StatisticValues.stringValue("a"));
    reverseSortedMap.put("b", StatisticValues.longValue(2L));
    reverseSortedMap.put("c", StatisticValues.booleanValue(true));
    StatisticValues.ObjectValue reverseSortedObjectValue =
        StatisticValues.objectValue(reverseSortedMap);

    Map<String, StatisticValue<?>> map = Maps.newHashMap();
    map.put("a", StatisticValues.stringValue("a"));
    map.put("b", StatisticValues.longValue(2L));
    map.put("c", StatisticValues.booleanValue(true));
    StatisticValues.ObjectValue sortedObjectValue = StatisticValues.objectValue(map);

    Assertions.assertEquals(reverseSortedObjectValue, sortedObjectValue);
  }
}
