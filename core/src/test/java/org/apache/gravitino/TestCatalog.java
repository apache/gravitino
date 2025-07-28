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

import static org.apache.gravitino.connector.TestCatalogOperations.FAIL_CREATE;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.BaseCatalogPropertiesMetadata;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.connector.TestCatalogOperations;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.rel.TableCatalog;

public class TestCatalog extends BaseCatalog<TestCatalog> {

  private static final TestBasePropertiesMetadata BASE_PROPERTIES_METADATA =
      new TestBasePropertiesMetadata();

  private static final TestFilesetPropertiesMetadata FILESET_PROPERTIES_METADATA =
      new TestFilesetPropertiesMetadata();

  public static final String PROPERTY_KEY1 = "key1";
  public static final String PROPERTY_KEY2 = "key2";
  public static final String PROPERTY_KEY3 = "key3";
  public static final String PROPERTY_KEY4 = "key4";
  public static final String PROPERTY_RESERVED_KEY = "reserved_key";
  public static final String PROPERTY_HIDDEN_KEY = "hidden_key";
  public static final String PROPERTY_KEY5_PREFIX = "key5-";
  public static final String PROPERTY_KEY6_PREFIX = "key6-";

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
    return new BaseCatalogPropertiesMetadata() {
      @Override
      protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
        return ImmutableMap.<String, PropertyEntry<?>>builder()
            .put(
                PROPERTY_KEY1,
                PropertyEntry.stringPropertyEntry(
                    PROPERTY_KEY1,
                    "value1" /* description */,
                    true /* required */,
                    true /* immutable */,
                    null /* default value*/,
                    false /* hidden */,
                    false /* reserved */))
            .put(
                PROPERTY_KEY2,
                PropertyEntry.stringPropertyEntry(
                    PROPERTY_KEY2,
                    "value2" /* description */,
                    true /* required */,
                    false /* immutable */,
                    null /* default value*/,
                    false /* hidden */,
                    false /* reserved */))
            .put(
                PROPERTY_KEY3,
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
                    .withName(PROPERTY_KEY3)
                    .build())
            .put(
                PROPERTY_KEY4,
                PropertyEntry.stringPropertyEntry(
                    PROPERTY_KEY4, "value4", false, false, "value4", false, false))
            .put(
                PROPERTY_RESERVED_KEY,
                PropertyEntry.stringPropertyEntry(
                    PROPERTY_RESERVED_KEY,
                    "reserved_key" /* description */,
                    false /* required */,
                    true /* immutable */,
                    "reserved_value" /* default value*/,
                    false /* hidden */,
                    true /* reserved */))
            .put(
                PROPERTY_HIDDEN_KEY,
                PropertyEntry.stringPropertyEntry(
                    PROPERTY_HIDDEN_KEY,
                    "hidden_key" /* description */,
                    false /* required */,
                    false /* immutable */,
                    "hidden_value" /* default value*/,
                    true /* hidden */,
                    false /* reserved */))
            .put(
                FAIL_CREATE,
                PropertyEntry.booleanPropertyEntry(
                    FAIL_CREATE,
                    "Whether an exception needs to be thrown on creation",
                    false /* required */,
                    false /* immutable */,
                    false /* default value*/,
                    false /* hidden */,
                    false /* reserved */))
            .put(
                PROPERTY_KEY5_PREFIX,
                PropertyEntry.stringRequiredPropertyPrefixEntry(
                    PROPERTY_KEY5_PREFIX,
                    "property with prefix 'key5-'",
                    false /* immutable */,
                    null /* default value*/,
                    false /* hidden */,
                    false /* reserved */))
            .put(
                PROPERTY_KEY6_PREFIX,
                PropertyEntry.stringImmutablePropertyPrefixEntry(
                    PROPERTY_KEY6_PREFIX,
                    "property with prefix 'key6-'",
                    false /* required */,
                    null /* default value*/,
                    false /* hidden */,
                    false /* reserved */))
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

  @Override
  public PropertiesMetadata modelPropertiesMetadata() throws UnsupportedOperationException {
    return BASE_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata modelVersionPropertiesMetadata() throws UnsupportedOperationException {
    return BASE_PROPERTIES_METADATA;
  }
}
