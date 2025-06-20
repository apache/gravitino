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
}
