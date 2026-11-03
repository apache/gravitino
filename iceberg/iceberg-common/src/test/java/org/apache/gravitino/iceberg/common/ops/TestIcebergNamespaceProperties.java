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
package org.apache.gravitino.iceberg.common.ops;

import com.google.common.collect.ImmutableMap;
import java.nio.file.Path;
import java.util.Map;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestIcebergNamespaceProperties {

  @TempDir Path warehouse;

  @ParameterizedTest
  @ValueSource(strings = {"x", "\uD83D\uDE80"})
  void testJdbcNamespacePropertyLengthBoundary(String character) throws Exception {
    try (IcebergCatalogWrapper wrapper = jdbcWrapper()) {
      Namespace valid = Namespace.of("valid");
      String allowedValue = character.repeat(1000);
      wrapper.createNamespace(
          CreateNamespaceRequest.builder()
              .withNamespace(valid)
              .setProperties(ImmutableMap.of("big", allowedValue))
              .build());
      Assertions.assertEquals(allowedValue, wrapper.loadNamespace(valid).properties().get("big"));

      Namespace invalid = Namespace.of("invalid");
      IllegalArgumentException error =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  wrapper.createNamespace(
                      CreateNamespaceRequest.builder()
                          .withNamespace(invalid)
                          .setProperties(ImmutableMap.of("big", character.repeat(1001)))
                          .build()));
      Assertions.assertTrue(error.getMessage().contains("big"));
      Assertions.assertTrue(error.getMessage().contains("1000"));
      Assertions.assertFalse(wrapper.namespaceExists(invalid));
    }
  }

  @Test
  void testInvalidUpdateDoesNotApplyRemovalsOrOtherUpdates() throws Exception {
    try (IcebergCatalogWrapper wrapper = jdbcWrapper()) {
      Namespace namespace = Namespace.of("updates");
      Map<String, String> original = ImmutableMap.of("big", "original", "keep", "value");
      wrapper.createNamespace(
          CreateNamespaceRequest.builder()
              .withNamespace(namespace)
              .setProperties(original)
              .build());
      Map<String, String> before =
          ImmutableMap.copyOf(wrapper.loadNamespace(namespace).properties());
      UpdateNamespacePropertiesRequest invalid =
          UpdateNamespacePropertiesRequest.builder()
              .remove("keep")
              .update("new", "value")
              .update("big", "x".repeat(1001))
              .build();
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> wrapper.updateNamespaceProperties(namespace, invalid));
      Assertions.assertEquals(before, wrapper.loadNamespace(namespace).properties());

      wrapper.updateNamespaceProperties(
          namespace,
          UpdateNamespacePropertiesRequest.builder().update("big", "x".repeat(1000)).build());
      Assertions.assertEquals(
          1000, wrapper.loadNamespace(namespace).properties().get("big").length());
    }
  }

  @Test
  void testOtherBackendsRetainLargerProperties() throws Exception {
    try (IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(Map.of("catalog-backend", "memory")))) {
      Namespace namespace = Namespace.of("large");
      wrapper.createNamespace(
          CreateNamespaceRequest.builder()
              .withNamespace(namespace)
              .setProperties(ImmutableMap.of("big", "x".repeat(1001)))
              .build());
      wrapper.updateNamespaceProperties(
          namespace,
          UpdateNamespacePropertiesRequest.builder().update("big", "x".repeat(2000)).build());
      Assertions.assertEquals(
          2000, wrapper.loadNamespace(namespace).properties().get("big").length());
    }
  }

  private IcebergCatalogWrapper jdbcWrapper() {
    return new IcebergCatalogWrapper(
        new IcebergConfig(
            Map.of(
                "catalog-backend",
                "jdbc",
                "jdbc-driver",
                "org.sqlite.JDBC",
                "uri",
                "jdbc:sqlite:" + warehouse.resolve("catalog.db"),
                "warehouse",
                warehouse.toString())));
  }
}
