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
package org.apache.gravitino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import org.junit.jupiter.api.Test;

public class TestBasePropertiesMetadata extends BasePropertiesMetadata {

  public static final String COMMENT_KEY = "comment";

  public static final String TEST_REQUIRED_KEY = "k1";

  public static final String TEST_IMMUTABLE_KEY = "immutableKey";

  private static final Map<String, PropertyEntry<?>> TEST_BASE_PROPERTY;

  static {
    List<PropertyEntry<?>> tablePropertyMetadata =
        ImmutableList.of(
            PropertyEntry.stringRequiredPropertyEntry(
                TEST_REQUIRED_KEY, "test required k1 property", false, false),
            PropertyEntry.stringReservedPropertyEntry(COMMENT_KEY, "table comment", true),
            PropertyEntry.stringImmutablePropertyEntry(
                TEST_IMMUTABLE_KEY, "test immutable property", false, null, false, false));

    TEST_BASE_PROPERTY = Maps.uniqueIndex(tablePropertyMetadata, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return TEST_BASE_PROPERTY;
  }

  @Test
  public void testGetPropertyEntryWithExistingProperty() {
    TestBasePropertiesMetadata metadata = new TestBasePropertiesMetadata();

    PropertyEntry<?> entry =
        metadata.getPropertyEntry(TestBasePropertiesMetadata.TEST_REQUIRED_KEY);
    assertNotNull(entry);
    assertEquals(TestBasePropertiesMetadata.TEST_REQUIRED_KEY, entry.getName());
    assertEquals("test required k1 property", entry.getDescription());
    assertTrue(entry.isRequired());
  }

  @Test
  public void testGetPropertyEntryWithNonExistentProperty() {
    TestBasePropertiesMetadata metadata = new TestBasePropertiesMetadata();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> metadata.getPropertyEntry("non-existent-property"));
    assertTrue(exception.getMessage().contains("Property is not defined"));
  }

  @Test
  public void testGetPropertyEntryWithInvalidInput() {
    TestBasePropertiesMetadata metadata = new TestBasePropertiesMetadata();

    IllegalArgumentException nullException =
        assertThrows(IllegalArgumentException.class, () -> metadata.getPropertyEntry(null));
    assertTrue(nullException.getMessage().contains("Property is not defined"));

    IllegalArgumentException emptyException =
        assertThrows(IllegalArgumentException.class, () -> metadata.getPropertyEntry(""));
    assertTrue(emptyException.getMessage().contains("Property is not defined"));
  }
}
