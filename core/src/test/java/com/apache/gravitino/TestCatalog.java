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
package com.apache.gravitino;

import static com.apache.gravitino.connector.TestCatalogOperations.FAIL_CREATE;

import com.apache.gravitino.connector.BaseCatalog;
import com.apache.gravitino.connector.BasePropertiesMetadata;
import com.apache.gravitino.connector.CatalogOperations;
import com.apache.gravitino.connector.PropertiesMetadata;
import com.apache.gravitino.connector.PropertyEntry;
import com.apache.gravitino.connector.TestCatalogOperations;
import com.apache.gravitino.connector.capability.Capability;
import com.apache.gravitino.rel.TableCatalog;
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
  public TableCatalog asTableCatalog() {
    return (TableCatalog) ops();
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
            .put(
                AUTHORIZATION_PROVIDER,
                PropertyEntry.stringImmutablePropertyEntry(
                    Catalog.AUTHORIZATION_PROVIDER,
                    "The name of the authorization provider for Gravitino",
                    false,
                    null,
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
