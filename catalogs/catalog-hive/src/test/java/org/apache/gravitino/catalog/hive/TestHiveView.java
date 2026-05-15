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
package org.apache.gravitino.catalog.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TestHiveView {

  @Test
  void testBuildWithoutPropertiesReturnsEmptyMap() {
    HiveView view = HiveView.builder().withName("v1").build();

    assertNotNull(view.properties());
    assertTrue(view.properties().isEmpty());
  }

  @Test
  void testBuildCopiesPropertiesMap() {
    Map<String, String> sourceProperties = new HashMap<>();
    sourceProperties.put("k1", "v1");

    HiveView view = HiveView.builder().withName("v1").withProperties(sourceProperties).build();
    sourceProperties.put("k2", "v2");

    assertEquals(1, view.properties().size());
    assertEquals("v1", view.properties().get("k1"));
  }
}
