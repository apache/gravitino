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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.FilesetEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestEntityCombinedFileset {

  /**
   * This test has no real assertions, but it's just to use the getter methods, by calling them
   * others will not delete the getters by mistake.
   */
  @Test
  void testGetters() {
    Fileset fileset = Mockito.mock(Fileset.class);
    FilesetEntity filesetEntity =
        FilesetEntity.builder()
            .withId(1L)
            .withName("fileset")
            .withAuditInfo(AuditInfo.EMPTY)
            .withFilesetType(Fileset.Type.MANAGED)
            .withStorageLocations(ImmutableMap.of(LOCATION_NAME_UNKNOWN, "testLocation"))
            .withProperties(ImmutableMap.of())
            .build();

    EntityCombinedFileset entityCombinedFileset = EntityCombinedFileset.of(fileset, filesetEntity);
    Assertions.assertEquals(fileset, entityCombinedFileset.fileset());
    Assertions.assertEquals(filesetEntity, entityCombinedFileset.filesetEntity());
  }

  /**
   * Test that properties() method works correctly when hiddenProperties is not initialized. This
   * test verifies the fix for NPE issue #8168.
   */
  @Test
  void testPropertiesWithoutHiddenProperties() {
    Fileset fileset = Mockito.mock(Fileset.class);
    ImmutableMap<String, String> properties = ImmutableMap.of("propA", "valueA", "propB", "valueB");
    Mockito.when(fileset.properties()).thenReturn(properties);

    EntityCombinedFileset entityCombinedFileset = EntityCombinedFileset.of(fileset);

    // This should not throw NPE and should return all properties
    Assertions.assertEquals(properties, entityCombinedFileset.properties());
  }

  /** Test that properties() method correctly filters hidden properties. */
  @Test
  void testPropertiesWithHiddenProperties() {
    Fileset fileset = Mockito.mock(Fileset.class);
    ImmutableMap<String, String> properties =
        ImmutableMap.of("propA", "valueA", "propB", "valueB", "hiddenProp", "hiddenValue");
    Mockito.when(fileset.properties()).thenReturn(properties);

    EntityCombinedFileset entityCombinedFileset =
        EntityCombinedFileset.of(fileset).withHiddenProperties(ImmutableSet.of("hiddenProp"));

    Map<String, String> result = entityCombinedFileset.properties();

    // Should only contain non-hidden properties
    Assertions.assertEquals(2, result.size());
    Assertions.assertEquals("valueA", result.get("propA"));
    Assertions.assertEquals("valueB", result.get("propB"));
    Assertions.assertNull(result.get("hiddenProp"));
  }

  /** Test that withHiddenProperties() method handles null input correctly. */
  @Test
  void testWithHiddenPropertiesNull() {
    Fileset fileset = Mockito.mock(Fileset.class);
    ImmutableMap<String, String> properties = ImmutableMap.of("propA", "valueA", "propB", "valueB");
    Mockito.when(fileset.properties()).thenReturn(properties);

    EntityCombinedFileset entityCombinedFileset =
        EntityCombinedFileset.of(fileset).withHiddenProperties(null);

    // Should not throw NPE and should return all properties
    Assertions.assertEquals(properties, entityCombinedFileset.properties());
  }

  /** Test that properties() method handles null values correctly in the property map. */
  @Test
  void testPropertiesWithNullValues() {
    Fileset fileset = Mockito.mock(Fileset.class);
    Map<String, String> propertiesWithNull =
        ImmutableMap.<String, String>builder()
            .put("propA", "valueA")
            .put("propB", "valueB")
            .build();

    // Mock a map that includes null key/value (though ImmutableMap doesn't allow nulls,
    // this simulates what might happen with other Map implementations)
    Map<String, String> mockProperties = Mockito.mock(Map.class);
    Mockito.when(mockProperties.entrySet()).thenReturn(propertiesWithNull.entrySet());
    Mockito.when(fileset.properties()).thenReturn(mockProperties);

    EntityCombinedFileset entityCombinedFileset = EntityCombinedFileset.of(fileset);

    Map<String, String> result = entityCombinedFileset.properties();

    // Should contain all valid properties (no null keys/values)
    Assertions.assertEquals(2, result.size());
    Assertions.assertEquals("valueA", result.get("propA"));
    Assertions.assertEquals("valueB", result.get("propB"));
  }
}
