/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

public class TestFilesetPropertiesMetadata extends TestBasePropertiesMetadata {

  public static final String TEST_FILESET_HIDDEN_KEY = "fileset_key";

  private static final Map<String, PropertyEntry<?>> TEST_FILESET_PROPERTY;

  static {
    List<PropertyEntry<?>> tablePropertyMetadata =
        ImmutableList.of(
            PropertyEntry.stringPropertyEntry(
                TEST_FILESET_HIDDEN_KEY,
                "test fileset required k1 property",
                false,
                false,
                "test",
                true,
                false));

    TEST_FILESET_PROPERTY = Maps.uniqueIndex(tablePropertyMetadata, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return ImmutableMap.<String, PropertyEntry<?>>builder()
        .putAll(super.specificPropertyEntries())
        .putAll(TEST_FILESET_PROPERTY)
        .build();
  }
}
