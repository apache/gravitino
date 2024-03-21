/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

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
}
