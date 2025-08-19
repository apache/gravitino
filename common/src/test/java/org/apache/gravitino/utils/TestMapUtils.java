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

package org.apache.gravitino.utils;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMapUtils {

  @Test
  public void testGetPrefixMap() {
    Map configs = ImmutableMap.of("a.b", "", "a.c", "", "", "", "b.a", "");
    Assertions.assertEquals(
        ImmutableMap.of("a.b", "", "a.c", ""), MapUtils.getPrefixMap(configs, "a.", true));
    Assertions.assertEquals(
        ImmutableMap.of("b", "", "c", ""), MapUtils.getPrefixMap(configs, "a.", false));

    Assertions.assertEquals(ImmutableMap.of("b.a", ""), MapUtils.getPrefixMap(configs, "b.", true));
    Assertions.assertEquals(ImmutableMap.of("a", ""), MapUtils.getPrefixMap(configs, "b.", false));

    Assertions.assertEquals(configs, MapUtils.getPrefixMap(configs, "", true));
    Assertions.assertEquals(configs, MapUtils.getPrefixMap(configs, "", false));

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> MapUtils.getPrefixMap(configs, null, true));
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> MapUtils.getPrefixMap(null, "", true));
  }

  @Test
  void testGetMapWithoutPrefix() {
    Map<String, String> properties =
        ImmutableMap.of(
            "location-a",
            "path-a",
            "location-b",
            "path-b",
            "location-c",
            "path-c",
            "property-a",
            "value-a",
            "property-b",
            "value-b",
            "property-c",
            "value-c");

    Assertions.assertEquals(
        ImmutableMap.of("property-a", "value-a", "property-b", "value-b", "property-c", "value-c"),
        MapUtils.getMapWithoutPrefix(properties, "location-"));

    Assertions.assertEquals(
        ImmutableMap.of("location-a", "path-a", "location-b", "path-b", "location-c", "path-c"),
        MapUtils.getMapWithoutPrefix(properties, "property-"));
    Assertions.assertEquals(properties, MapUtils.getMapWithoutPrefix(properties, "unknown-"));
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> MapUtils.getMapWithoutPrefix(properties, null));
  }
}
