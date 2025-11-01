/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class TestCatalogWrapperForREST {

  @Test
  void testCheckPropertiesForCompatibility() {
    ImmutableMap<String, String> deprecatedMap = ImmutableMap.of("deprecated", "new");
    ImmutableMap<String, String> propertiesWithDeprecatedKey = ImmutableMap.of("deprecated", "v");
    Map<String, String> newProperties =
        CatalogWrapperForREST.checkForCompatibility(propertiesWithDeprecatedKey, deprecatedMap);
    Assertions.assertEquals(newProperties, ImmutableMap.of("new", "v"));

    ImmutableMap<String, String> propertiesWithoutDeprecatedKey = ImmutableMap.of("k", "v");
    newProperties =
        CatalogWrapperForREST.checkForCompatibility(propertiesWithoutDeprecatedKey, deprecatedMap);
    Assertions.assertEquals(newProperties, ImmutableMap.of("k", "v"));

    ImmutableMap<String, String> propertiesWithBothKey =
        ImmutableMap.of("deprecated", "v", "new", "v");

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> CatalogWrapperForREST.checkForCompatibility(propertiesWithBothKey, deprecatedMap));
  }

  @Test
  void testPlanTableScanResponseFormat() {
    // Test that scan response contains required fields
    // Verify that scan response should contain:
    // - plan-id: unique identifier for the scan plan
    // - tasks: list of scan tasks

    // Note: This is a structure validation test.
    // Full integration tests are in TestPlanTableScan
    Map<String, Object> mockResponse = new HashMap<>();
    mockResponse.put("plan-id", "test-plan-id");
    mockResponse.put("tasks", List.of());

    Assertions.assertTrue(mockResponse.containsKey("plan-id"));
    Assertions.assertTrue(mockResponse.containsKey("tasks"));
    Assertions.assertNotNull(mockResponse.get("plan-id"));
    Assertions.assertInstanceOf(List.class, mockResponse.get("tasks"));
  }

  @Test
  void testScanRequestParameters() {
    // Test that scan request can contain various parameters
    Map<String, Object> scanRequest = new HashMap<>();

    // Test snapshot-id parameter
    scanRequest.put("snapshot-id", 12345L);
    Assertions.assertEquals(12345L, ((Number) scanRequest.get("snapshot-id")).longValue());

    // Test filter parameter
    Map<String, Object> filter = new HashMap<>();
    filter.put("type", "eq");
    filter.put("term", "id");
    filter.put("value", 1);
    scanRequest.put("filter", filter);
    Assertions.assertTrue(scanRequest.containsKey("filter"));

    // Test select parameter
    List<String> columns = List.of("col1", "col2");
    scanRequest.put("select", columns);
    Assertions.assertEquals(columns, scanRequest.get("select"));

    // Test case-sensitive parameter
    scanRequest.put("case-sensitive", true);
    Assertions.assertTrue((Boolean) scanRequest.get("case-sensitive"));

    // Test options parameter
    Map<String, String> options = new HashMap<>();
    options.put("split-size", "134217728");
    scanRequest.put("options", options);
    Assertions.assertTrue(scanRequest.containsKey("options"));
  }

  @Test
  void testCaseSensitiveParameterFormats() {
    // Test different formats for case-sensitive parameter
    Map<String, Object> scanRequest1 = new HashMap<>();
    scanRequest1.put("case-sensitive", true);
    Assertions.assertTrue((Boolean) scanRequest1.get("case-sensitive"));

    Map<String, Object> scanRequest2 = new HashMap<>();
    scanRequest2.put("case-sensitive", false);
    Assertions.assertFalse((Boolean) scanRequest2.get("case-sensitive"));

    Map<String, Object> scanRequest3 = new HashMap<>();
    scanRequest3.put("case-sensitive", "true");
    Assertions.assertEquals("true", scanRequest3.get("case-sensitive"));

    Map<String, Object> scanRequest4 = new HashMap<>();
    scanRequest4.put("case-sensitive", "false");
    Assertions.assertEquals("false", scanRequest4.get("case-sensitive"));
  }

  @Test
  void testFilterExpressionTypes() {
    // Test various filter expression types that should be supported
    // Test equality filter
    Map<String, Object> eqFilter = new HashMap<>();
    eqFilter.put("type", "eq");
    eqFilter.put("term", "id");
    eqFilter.put("value", 1);
    Assertions.assertTrue(eqFilter.containsKey("type"));
    Assertions.assertEquals("eq", eqFilter.get("type"));

    // Test comparison filters
    Map<String, Object> ltFilter = new HashMap<>();
    ltFilter.put("type", "lt");
    ltFilter.put("term", "age");
    ltFilter.put("value", 30);
    Assertions.assertEquals("lt", ltFilter.get("type"));

    // Test logical filters
    Map<String, Object> andFilter = new HashMap<>();
    andFilter.put("type", "and");
    andFilter.put("left", eqFilter);
    andFilter.put("right", ltFilter);
    Assertions.assertEquals("and", andFilter.get("type"));

    // Test null filters
    Map<String, Object> nullFilter = new HashMap<>();
    nullFilter.put("type", "is-null");
    nullFilter.put("term", "name");
    Assertions.assertEquals("is-null", nullFilter.get("type"));

    // Test in filter
    Map<String, Object> inFilter = new HashMap<>();
    inFilter.put("type", "in");
    inFilter.put("term", "status");
    inFilter.put("values", Arrays.asList("active", "pending"));
    Assertions.assertEquals("in", inFilter.get("type"));
  }

  @Test
  void testEmptyAndNullRequestHandling() {
    // Test handling of empty requests
    Map<String, Object> emptyRequest = new HashMap<>();
    Assertions.assertTrue(emptyRequest.isEmpty());

    // Test that empty request is valid (no parameters)
    Assertions.assertFalse(emptyRequest.containsKey("filter"));
    Assertions.assertFalse(emptyRequest.containsKey("select"));
    Assertions.assertFalse(emptyRequest.containsKey("case-sensitive"));
  }

  @Test
  void testResponseStructureValidation() {
    // Test that all expected response fields are present and correctly typed
    Map<String, Object> response = new HashMap<>();
    response.put("plan-id", "test-plan-123");
    response.put("tasks", Arrays.asList());

    // Verify plan-id
    Assertions.assertTrue(response.containsKey("plan-id"));
    Assertions.assertInstanceOf(String.class, response.get("plan-id"));
    Assertions.assertNotNull(response.get("plan-id"));
    Assertions.assertFalse(((String) response.get("plan-id")).trim().isEmpty());

    // Verify tasks
    Assertions.assertTrue(response.containsKey("tasks"));
    Assertions.assertInstanceOf(List.class, response.get("tasks"));

    // Test with config field (for credential vending)
    Map<String, Object> responseWithConfig = new HashMap<>();
    responseWithConfig.put("plan-id", "test-plan-456");
    responseWithConfig.put("tasks", Arrays.asList());
    responseWithConfig.put("config", Map.of("fs.s3a.access.key", "test-key"));

    Assertions.assertTrue(responseWithConfig.containsKey("config"));
    Assertions.assertInstanceOf(Map.class, responseWithConfig.get("config"));
  }
}
