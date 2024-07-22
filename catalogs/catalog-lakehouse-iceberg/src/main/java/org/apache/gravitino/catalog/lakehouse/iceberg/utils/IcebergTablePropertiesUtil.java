/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.catalog.lakehouse.iceberg.utils;

import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTable.DEFAULT_ICEBERG_PROVIDER;
import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata.CURRENT_SNAPSHOT_ID;
import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata.FORMAT;
import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata.FORMAT_VERSION;
import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata.IDENTIFIER_FIELDS;
import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata.LOCATION;
import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata.PROVIDER;
import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata.SORT_ORDER;

import com.google.common.base.Joiner;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.catalog.lakehouse.iceberg.converter.DescribeIcebergSortOrderVisitor;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.transforms.SortOrderVisitor;

/** Referred from org/apache/iceberg/spark/source/SparkTable.java#properties() */
public class IcebergTablePropertiesUtil {

  public static Map<String, String> buildReservedProperties(TableMetadata table) {
    Map<String, String> properties = new HashMap<>();
    String fileFormat =
        table
            .properties()
            .getOrDefault(
                TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    properties.put(FORMAT, String.join("/", DEFAULT_ICEBERG_PROVIDER, fileFormat));
    properties.put(PROVIDER, DEFAULT_ICEBERG_PROVIDER);
    String currentSnapshotId =
        table.currentSnapshot() != null
            ? String.valueOf(table.currentSnapshot().snapshotId())
            : "none";
    properties.put(CURRENT_SNAPSHOT_ID, currentSnapshotId);
    properties.put(LOCATION, table.location());

    properties.put(FORMAT_VERSION, String.valueOf(table.formatVersion()));

    if (!table.sortOrder().isUnsorted()) {
      properties.put(SORT_ORDER, describeIcebergSortOrder(table.sortOrder()));
    }

    Set<String> identifierFields = table.schema().identifierFieldNames();
    if (!identifierFields.isEmpty()) {
      properties.put(IDENTIFIER_FIELDS, "[" + String.join(",", identifierFields) + "]");
    }

    return properties;
  }

  private static String describeIcebergSortOrder(org.apache.iceberg.SortOrder sortOrder) {
    return Joiner.on(", ")
        .join(SortOrderVisitor.visit(sortOrder, DescribeIcebergSortOrderVisitor.INSTANCE));
  }

  private IcebergTablePropertiesUtil() {}
}
