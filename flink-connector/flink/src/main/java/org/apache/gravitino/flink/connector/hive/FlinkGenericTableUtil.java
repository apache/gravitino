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
package org.apache.gravitino.flink.connector.hive;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.gravitino.rel.Table;

final class FlinkGenericTableUtil {
  public static final String CONNECTOR = FactoryUtil.CONNECTOR.key();
  private static final String CONNECTOR_TYPE = "connector.type";
  private static final String MANAGED_TABLE_IDENTIFIER = "default";

  private FlinkGenericTableUtil() {}

  static boolean isGenericTableWhenCreate(Map<String, String> options) {
    if (options == null) {
      return true;
    }
    String connector = options.get(CONNECTOR);
    if (connector == null) {
      return true;
    }
    return !"hive".equalsIgnoreCase(connector);
  }

  static boolean isGenericTableWhenLoad(Map<String, String> properties) {
    // If a table doesn't have properties, it is a raw hive table.
    if (properties == null) {
      return false;
    }
    if (properties.containsKey(CatalogPropertiesUtil.IS_GENERIC)) {
      return Boolean.parseBoolean(properties.get(CatalogPropertiesUtil.IS_GENERIC));
    }
    String connector = getConnectorFromProperties(properties);
    if (connector == null) {
      return false;
    }
    return !"hive".equalsIgnoreCase(connector);
  }

  static Map<String, String> toGravitinoGenericTableProperties(ResolvedCatalogTable resolvedTable) {
    Map<String, String> properties = CatalogPropertiesUtil.serializeCatalogTable(resolvedTable);
    if (!properties.containsKey(CONNECTOR)) {
      properties.put(CONNECTOR, MANAGED_TABLE_IDENTIFIER);
    }
    Map<String, String> masked = maskFlinkProperties(properties);
    masked.put(CatalogPropertiesUtil.IS_GENERIC, "true");
    return masked;
  }

  static CatalogTable toFlinkGenericTable(Table table) {
    Map<String, String> flinkProperties = unmaskFlinkProperties(table.properties());
    CatalogTable catalogTable = CatalogPropertiesUtil.deserializeCatalogTable(flinkProperties);
    if (catalogTable.getUnresolvedSchema().getColumns().isEmpty()) {
      catalogTable =
          CatalogPropertiesUtil.deserializeCatalogTable(flinkProperties, "generic.table.schema");
    }
    Map<String, String> options = new HashMap<>(catalogTable.getOptions());
    if (MANAGED_TABLE_IDENTIFIER.equalsIgnoreCase(options.get(CONNECTOR))) {
      options.remove(CONNECTOR);
    }
    return CatalogTable.of(
        catalogTable.getUnresolvedSchema(),
        catalogTable.getComment(),
        catalogTable.getPartitionKeys(),
        options);
  }

  private static String getConnectorFromProperties(Map<String, String> properties) {
    String connector = properties.get(CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX + CONNECTOR);
    if (connector == null) {
      connector = properties.get(CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX + CONNECTOR_TYPE);
    }
    return connector;
  }

  private static Map<String, String> maskFlinkProperties(Map<String, String> properties) {
    if (properties == null) {
      return Collections.emptyMap();
    }
    return properties.entrySet().stream()
        .filter(e -> e.getKey() != null && e.getValue() != null)
        .collect(
            Collectors.toMap(
                e -> CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX + e.getKey(),
                Map.Entry::getValue));
  }

  private static Map<String, String> unmaskFlinkProperties(Map<String, String> properties) {
    if (properties == null) {
      return Collections.emptyMap();
    }
    return properties.entrySet().stream()
        .filter(e -> e.getKey().startsWith(CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX))
        .collect(
            Collectors.toMap(
                e -> e.getKey().substring(CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX.length()),
                Map.Entry::getValue));
  }
}
