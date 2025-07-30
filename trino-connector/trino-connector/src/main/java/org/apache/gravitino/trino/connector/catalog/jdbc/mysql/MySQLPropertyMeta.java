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

package org.apache.gravitino.trino.connector.catalog.jdbc.mysql;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.jdbc.$internal.guava.collect.ImmutableMap;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.trino.connector.catalog.HasPropertyMeta;

/**
 * Property metadata for MySQL tables and columns. This class defines and manages the property
 * metadata for MySQL-specific configurations.
 */
public class MySQLPropertyMeta implements HasPropertyMeta {

  static final String TABLE_ENGINE = "engine";
  static final String TABLE_AUTO_INCREMENT_OFFSET = "auto_increment_offset";
  public static final String TABLE_PRIMARY_KEY = "primary_key";
  public static final String TABLE_UNIQUE_KEY = "unique_key";

  private static final List<PropertyMetadata<?>> TABLE_PROPERTY_META =
      ImmutableList.of(
          stringProperty(TABLE_ENGINE, "The engine that MySQL table uses", "InnoDB", false),
          stringProperty(
              TABLE_AUTO_INCREMENT_OFFSET, "The auto increment offset for the table", null, false),
          new PropertyMetadata<>(
              TABLE_PRIMARY_KEY,
              "The primary keys for the table",
              new ArrayType(VARCHAR),
              List.class,
              ImmutableList.of(),
              false,
              value -> (List<?>) value,
              value -> value),
          new PropertyMetadata<>(
              TABLE_UNIQUE_KEY,
              "The unique keys for the table",
              new ArrayType(VARCHAR),
              List.class,
              ImmutableList.of(),
              false,
              value -> (List<?>) value,
              value -> value));

  /** Property name for auto-incrementing columns. */
  public static final String AUTO_INCREMENT = "auto_increment";

  private static final List<PropertyMetadata<?>> COLUMN_PROPERTY_META =
      ImmutableList.of(booleanProperty(AUTO_INCREMENT, "The auto increment column", false, false));

  @Override
  public List<PropertyMetadata<?>> getTablePropertyMetadata() {
    return TABLE_PROPERTY_META;
  }

  @Override
  public List<PropertyMetadata<?>> getColumnPropertyMetadata() {
    return COLUMN_PROPERTY_META;
  }

  /**
   * Extract primary key from table properties
   *
   * @param tableProperties table properties
   * @return primary key list
   */
  public static Set<String> getPrimaryKey(Map<String, Object> tableProperties) {
    Preconditions.checkArgument(tableProperties != null, "tableProperties is null");
    ImmutableSet.Builder<String> primaryKeyBuilder = new ImmutableSet.Builder<>();

    if (tableProperties.containsKey(TABLE_PRIMARY_KEY)) {
      primaryKeyBuilder.addAll((List<String>) tableProperties.get(TABLE_PRIMARY_KEY));
    }

    return primaryKeyBuilder.build();
  }

  /**
   * Extract unique key from table properties
   *
   * @param tableProperties table properties
   * @return unique key list
   */
  public static Map<String, Set<String>> getUniqueKey(Map<String, Object> tableProperties) {
    Preconditions.checkArgument(tableProperties != null, "tableProperties is null");
    ImmutableMap.Builder<String, Set<String>> uniqueKeyMapBuilder = new ImmutableMap.Builder<>();

    if (tableProperties.containsKey(TABLE_UNIQUE_KEY)) {
      List<String> uniqueKeyList = (List<String>) tableProperties.get(TABLE_UNIQUE_KEY);
      Splitter uniqueKeyDefSplitter = Splitter.on(':').trimResults().omitEmptyStrings();
      Splitter columnSplitter = Splitter.on(',').trimResults().omitEmptyStrings();

      for (String uniqueKeyDef : uniqueKeyList) {
        List<String> uniqueKeyDefSplit = uniqueKeyDefSplitter.splitToList(uniqueKeyDef);
        Preconditions.checkArgument(
            uniqueKeyDefSplit.size() == 2, "Invalid unique key define: %s", uniqueKeyDef);
        uniqueKeyMapBuilder.put(
            uniqueKeyDefSplit.get(0),
            ImmutableSet.copyOf(columnSplitter.splitToList(uniqueKeyDefSplit.get(1))));
      }
    }

    return uniqueKeyMapBuilder.build();
  }
}
