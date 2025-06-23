package org.apache.gravitino.stats;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
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

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            StatisticValues.listValue(
                Lists.newArrayList(booleanValue, longValue, doubleValue, stringValue)));

    List<StatisticValue> list =
        Lists.newArrayList(StatisticValues.stringValue("a"), StatisticValues.stringValue("b"));
    StatisticValues.ListValue listValue = StatisticValues.listValue(list);
    Assertions.assertEquals(2, listValue.value().size());
    Assertions.assertEquals(listValue.dataType(), Types.ListType.nullable(Types.StringType.get()));
    Assertions.assertIterableEquals(list, listValue.value());

    Map<String, StatisticValue> map = Maps.newHashMap();
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
}
