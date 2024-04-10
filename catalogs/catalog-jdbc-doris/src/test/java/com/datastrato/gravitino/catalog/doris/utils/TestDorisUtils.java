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
  public void testGeneratePropertiesSql() {
    // Test when properties is null
    Map<String, String> properties = null;
    String result = DorisUtils.generatePropertiesSql(properties);
    Assertions.assertEquals("", result);

    // Test when properties is empty
    properties = Collections.emptyMap();
    result = DorisUtils.generatePropertiesSql(properties);
    Assertions.assertEquals("", result);

    // Test when properties has single entry
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

  @Test
  public void testExtractTablePropertiesFromSql() {
    // Test when properties is null
    String createTableSql =
        "CREATE TABLE `testTable` (\n`testColumn` STRING NOT NULL COMMENT 'test comment'\n) ENGINE=OLAP\nCOMMENT \"test comment\"";
    Map<String, String> result = DorisUtils.extractPropertiesFromSql(createTableSql);
    Assertions.assertTrue(result.isEmpty());

    // Test when properties exist
    createTableSql =
        "CREATE TABLE `testTable` (\n`testColumn` STRING NOT NULL COMMENT 'test comment'\n) ENGINE=OLAP\nCOMMENT \"test comment\"\nPROPERTIES (\n\"test_property\"=\"test_value\"\n)";
    result = DorisUtils.extractPropertiesFromSql(createTableSql);
    Assertions.assertEquals("test_value", result.get("test_property"));

    // Test when multiple properties exist
    createTableSql =
        "CREATE TABLE `testTable` (\n`testColumn` STRING NOT NULL COMMENT 'test comment'\n) ENGINE=OLAP\nCOMMENT \"test comment\"\nPROPERTIES (\n\"test_property1\"=\"test_value1\",\n\"test_property2\"=\"test_value2\"\n)";
    result = DorisUtils.extractPropertiesFromSql(createTableSql);
    Assertions.assertEquals("test_value1", result.get("test_property1"));
    Assertions.assertEquals("test_value2", result.get("test_property2"));
  }
}
