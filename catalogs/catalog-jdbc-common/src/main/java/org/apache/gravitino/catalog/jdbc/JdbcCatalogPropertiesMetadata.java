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
package org.apache.gravitino.catalog.jdbc;

import static org.apache.gravitino.connector.PropertyEntry.integerPropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringOptionalPropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringPropertyEntry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.connector.BaseCatalogPropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

public class JdbcCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {
  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  private static final List<String> JDBC_PROPERTIES =
      ImmutableList.of(
          JdbcConfig.JDBC_URL.getKey(),
          JdbcConfig.JDBC_DATABASE.getKey(),
          JdbcConfig.JDBC_DRIVER.getKey(),
          JdbcConfig.USERNAME.getKey(),
          JdbcConfig.PASSWORD.getKey(),
          JdbcConfig.POOL_MIN_SIZE.getKey(),
          JdbcConfig.POOL_MAX_SIZE.getKey());

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            stringPropertyEntry(
                JdbcConfig.JDBC_URL.getKey(),
                JdbcConfig.JDBC_URL.getDoc(),
                true /* required */,
                false /* immutable */,
                null /* defaultValue */,
                false /* hidden */,
                false /* reserved */),
            stringOptionalPropertyEntry(
                JdbcConfig.JDBC_DATABASE.getKey(),
                JdbcConfig.JDBC_DATABASE.getDoc(),
                false /* immutable */,
                null /* defaultValue */,
                false /* hidden */),
            stringPropertyEntry(
                JdbcConfig.JDBC_DRIVER.getKey(),
                JdbcConfig.JDBC_DRIVER.getDoc(),
                true /* required */,
                false /* immutable */,
                null /* defaultValue */,
                false /* hidden */,
                false /* reserved */),
            stringPropertyEntry(
                JdbcConfig.USERNAME.getKey(),
                JdbcConfig.USERNAME.getDoc(),
                true /* required */,
                false /* immutable */,
                null /* defaultValue */,
                false /* hidden */,
                false /* reserved */),
            stringPropertyEntry(
                JdbcConfig.PASSWORD.getKey(),
                JdbcConfig.PASSWORD.getDoc(),
                true /* required */,
                false /* immutable */,
                null /* defaultValue */,
                false /* hidden */,
                false /* reserved */),
            integerPropertyEntry(
                JdbcConfig.POOL_MIN_SIZE.getKey(),
                JdbcConfig.POOL_MIN_SIZE.getDoc(),
                false /* required */,
                false /* immutable */,
                JdbcConfig.POOL_MIN_SIZE.getDefaultValue(),
                true /* hidden */,
                false /* reserved */),
            integerPropertyEntry(
                JdbcConfig.POOL_MAX_SIZE.getKey(),
                JdbcConfig.POOL_MAX_SIZE.getDoc(),
                false /* required */,
                false /* immutable */,
                JdbcConfig.POOL_MAX_SIZE.getDefaultValue(),
                true /* hidden */,
                false /* reserved */));
    PROPERTIES_METADATA =
        ImmutableMap.<String, PropertyEntry<?>>builder()
            .putAll(Maps.uniqueIndex(propertyEntries, PropertyEntry::getName))
            .build();
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }

  public Map<String, String> transformProperties(Map<String, String> properties) {
    Map<String, String> result = Maps.newHashMap();
    properties.forEach(
        (key, value) -> {
          if (JDBC_PROPERTIES.contains(key)) {
            result.put(key, value);
          }
        });
    return result;
  }
}
