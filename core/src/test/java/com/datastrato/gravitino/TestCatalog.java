/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import static com.datastrato.gravitino.connector.TestCatalogOperations.FAIL_CREATE;

import com.datastrato.gravitino.connector.BaseCatalog;
import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.datastrato.gravitino.connector.TestCatalogOperations;
import com.datastrato.gravitino.connector.capability.Capability;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class TestCatalog extends BaseCatalog<TestCatalog> {

  private static final TestBasePropertiesMetadata BASE_PROPERTIES_METADATA =
      new TestBasePropertiesMetadata();

  private static final TestFilesetPropertiesMetadata FILESET_PROPERTIES_METADATA =
      new TestFilesetPropertiesMetadata();

  public TestCatalog() {}

  @Override
  public String shortName() {
    return "test";
  }

  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    return new TestCatalogOperations(config);
  }

  @Override
  protected Capability newCapability() {
    return new TestCatalogCapabilities();
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return BASE_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return new BasePropertiesMetadata() {
      @Override
      protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
        return ImmutableMap.<String, PropertyEntry<?>>builder()
            .put(
                "key1",
                PropertyEntry.stringPropertyEntry("key1", "value1", true, true, null, false, false))
            .put(
                "key2",
                PropertyEntry.stringPropertyEntry(
                    "key2", "value2", true, false, null, false, false))
            .put(
                "key3",
                new PropertyEntry.Builder<Integer>()
                    .withDecoder(Integer::parseInt)
                    .withEncoder(Object::toString)
                    .withDefaultValue(1)
                    .withDescription("key3")
                    .withHidden(false)
                    .withReserved(false)
                    .withImmutable(true)
                    .withJavaType(Integer.class)
                    .withRequired(false)
                    .withName("key3")
                    .build())
            .put(
                "key4",
                PropertyEntry.stringPropertyEntry(
                    "key4", "value4", false, false, "value4", false, false))
            .put(
                "reserved_key",
                PropertyEntry.stringPropertyEntry(
                    "reserved_key", "reserved_key", false, true, "reserved_value", false, true))
            .put(
                "hidden_key",
                PropertyEntry.stringPropertyEntry(
                    "hidden_key", "hidden_key", false, false, "hidden_value", true, false))
            .put(
                FAIL_CREATE,
                PropertyEntry.booleanPropertyEntry(
                    FAIL_CREATE,
                    "Whether an exception needs to be thrown on creation",
                    false,
                    false,
                    false,
                    false,
                    false))
            .build();
      }
    };
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return BASE_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata filesetPropertiesMetadata() throws UnsupportedOperationException {
    return FILESET_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata topicPropertiesMetadata() throws UnsupportedOperationException {
    return BASE_PROPERTIES_METADATA;
  }
}
