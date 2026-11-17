/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Tests that the authorization filter returns listed namespaces unchanged. */
public class TestIcebergNamespaceListFilter {

  @Test
  public void testLevelContainingSeparatorIsNotSplit() {
    List<Namespace> listed =
        Arrays.asList(Namespace.of("x:y"), Namespace.of("x", "y"), Namespace.of("a"));
    ListNamespacesResponse response = ListNamespacesResponse.builder().addAll(listed).build();

    // Everything visible: the namespaces come back exactly as listed.
    Assertions.assertEquals(listed, filter(response, null).namespaces());

    // Only the Gravitino schema "a" is visible.
    Assertions.assertEquals(
        Arrays.asList(Namespace.of("a")),
        filter(response, Set.of(NameIdentifier.of("m", "c", "a"))).namespaces());
  }

  @SuppressWarnings("unchecked")
  private static ListNamespacesResponse filter(
      ListNamespacesResponse response, Set<NameIdentifier> visible) {
    try (MockedStatic<MetadataAuthzHelper> helper = Mockito.mockStatic(MetadataAuthzHelper.class)) {
      helper
          .when(
              () ->
                  MetadataAuthzHelper.filterByExpression(
                      anyString(), anyString(), any(), any(Namespace[].class), any(Function.class)))
          .thenAnswer(
              invocation -> {
                Namespace[] namespaces = invocation.getArgument(3);
                Function<Namespace, NameIdentifier> toIdent = invocation.getArgument(4);
                return Arrays.stream(namespaces)
                    .filter(ns -> visible == null || visible.contains(toIdent.apply(ns)))
                    .toArray(Namespace[]::new);
              });
      return IcebergNamespaceOperations.filterListNamespacesResponse(response, "m", "c");
    }
  }
}
