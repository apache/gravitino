/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDorisUtils {
  @Test
  public void generatePropertiesSql() {
    // Test When properties is null
    Map<String, String> properties = null;
    String result = DorisUtils.generatePropertiesSql(properties);
    Assertions.assertEquals("", result);

    // Test When properties is empty
    properties = Collections.emptyMap();
    result = DorisUtils.generatePropertiesSql(properties);
    Assertions.assertEquals("", result);

    // Test When properties has single entry
    properties = Collections.singletonMap("key", "value");
    result = DorisUtils.generatePropertiesSql(properties);
    Assertions.assertEquals(" PROPERTIES (\n\"key\"=\"value\"\n)", result);

    // Test when properties has multiple entries
    properties = new HashMap<>();
    properties.put("key1", "value1");
    properties.put("key2", "value2");

    String expectedStr = " PROPERTIES (\n\"key1\"=\"value1\",\n\"key2\"=\"value2\"\n)";

    result = DorisUtils.generatePropertiesSql(properties);
    Assertions.assertEquals(expectedStr, result);
  }
}
