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

package org.apache.gravitino.trino.connector.catalog.iceberg;

import static io.trino.spi.type.VarcharType.VARCHAR;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.trino.connector.catalog.HasPropertyMeta;

public class IcebergPropertyMeta implements HasPropertyMeta {

  public static final String ICEBERG_PARTITIONING_PROPERTY = "partitioning";
  public static final String ICEBERG_SORTED_BY_PROPERTY = "sorted_by";
  public static final String ICEBERG_LOCATION_PROPERTY = "location";

  // Value is whether this property is reserved and cannot be used by users
  // TODO (yuqi) add more properties
  public static final Map<PropertyMetadata<?>, Boolean> TABLE_PROPERTY_TO_RESERVED_MAP =
      new ImmutableMap.Builder().build();

  public static final List<PropertyMetadata<?>> TABLE_PROPERTY_META =
      ImmutableList.of(
          new PropertyMetadata<>(
              ICEBERG_PARTITIONING_PROPERTY,
              "Partition transforms",
              new ArrayType(VARCHAR),
              List.class,
              ImmutableList.of(),
              false,
              value -> (List<?>) value,
              value -> value),
          new PropertyMetadata<>(
              ICEBERG_SORTED_BY_PROPERTY,
              "Sorted columns",
              new ArrayType(VARCHAR),
              List.class,
              ImmutableList.of(),
              false,
              value -> (List<?>) value,
              value -> value),
          PropertyMetadata.stringProperty(
              ICEBERG_LOCATION_PROPERTY, "Location for table storage", null, false));

  // TODO (yuqi) add more properties
  public static final Map<PropertyMetadata<?>, Boolean> SCHEMA_PROPERTY_TO_RESERVED_MAP =
      new ImmutableMap.Builder().build();

  public static final List<PropertyMetadata<?>> SCHEMA_PROPERTY_META =
      SCHEMA_PROPERTY_TO_RESERVED_MAP.entrySet().stream()
          .map(Map.Entry::getKey)
          .collect(ImmutableList.toImmutableList());

  @Override
  public List<PropertyMetadata<?>> getSchemaPropertyMetadata() {
    return SCHEMA_PROPERTY_META;
  }

  @Override
  public List<PropertyMetadata<?>> getTablePropertyMetadata() {
    return TABLE_PROPERTY_META;
  }
}
