/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.operation;

import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test class for {@link DorisTableOperations}. */
public class TestDorisTableOperations {

  @Test
  public void testExtractTablePropertiesFromSql() {
    // Test When properties is null
    String createTableSql =
        "CREATE TABLE `testTable` (\n`testColumn` STRING NOT NULL COMMENT 'test comment'\n) ENGINE=OLAP\nCOMMENT \"test comment\"";
    Map<String, String> result = DorisTableOperations.extractTablePropertiesFromSql(createTableSql);
    Assertions.assertTrue(result.isEmpty());

    // Test When properties exist
    createTableSql =
        "CREATE TABLE `testTable` (\n`testColumn` STRING NOT NULL COMMENT 'test comment'\n) ENGINE=OLAP\nCOMMENT \"test comment\"\nPROPERTIES (\n\"test_property\"=\"test_value\"\n)";
    result = DorisTableOperations.extractTablePropertiesFromSql(createTableSql);
    Assertions.assertEquals("test_value", result.get("test_property"));

    // Test When multiple properties exist
    createTableSql =
        "CREATE TABLE `testTable` (\n`testColumn` STRING NOT NULL COMMENT 'test comment'\n) ENGINE=OLAP\nCOMMENT \"test comment\"\nPROPERTIES (\n\"test_property1\"=\"test_value1\",\n\"test_property2\"=\"test_value2\"\n)";
    result = DorisTableOperations.extractTablePropertiesFromSql(createTableSql);
    Assertions.assertEquals("test_value1", result.get("test_property1"));
    Assertions.assertEquals("test_value2", result.get("test_property2"));
  }
}
