/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestPropertiesConverter {

  @Test
  void testTransformOption() {
    Map properties = ImmutableMap.of("a", "b", "option.a1", "b");
    Map result = PropertiesConverter.transformOptionProperties(properties);
    Assertions.assertEquals(ImmutableMap.of("a", "b", "a1", "b"), result);
  }
}
