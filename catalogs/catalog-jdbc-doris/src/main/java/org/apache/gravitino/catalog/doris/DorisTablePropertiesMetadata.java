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
package org.apache.gravitino.catalog.doris;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.jdbc.JdbcTablePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

public class DorisTablePropertiesMetadata extends JdbcTablePropertiesMetadata {

  // ---- writable properties ----
  public static final String REPLICATION_FACTOR = "replication_num";
  public static final int DEFAULT_REPLICATION_FACTOR = 1;
  public static final int DEFAULT_REPLICATION_FACTOR_IN_SERVER_SIDE = 3;
  public static final String COMPRESSION = "compression";
  public static final String BLOOM_FILTER_COLUMNS = "bloom_filter_columns";
  public static final String STORAGE_POLICY = "storage_policy";
  // ---- reserved (read-only) properties ----
  public static final String LIGHT_SCHEMA_CHANGE = "light_schema_change";
  public static final String ENABLE_UNIQUE_KEY_MERGE_ON_WRITE = "enable_unique_key_merge_on_write";

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            PropertyEntry.integerOptionalPropertyEntry(
                REPLICATION_FACTOR,
                "The number of replications for the table. If not specified and the number"
                    + " of backend server less than 3, the default value will be used",
                false /* immutable */,
                DEFAULT_REPLICATION_FACTOR, /* default value */
                false /* hidden */),
            PropertyEntry.stringOptionalPropertyEntry(
                COMPRESSION,
                "The compression type for the table (ZSTD, LZ4, LZ4F, ZLIB)."
                    + " Deprecated as a table-level property in Doris 4.0+",
                false /* immutable */,
                null /* default value */,
                false /* hidden */),
            PropertyEntry.stringOptionalPropertyEntry(
                BLOOM_FILTER_COLUMNS,
                "Comma-separated list of columns for which bloom filter indexes are created",
                false /* immutable */,
                null /* default value */,
                false /* hidden */),
            PropertyEntry.stringOptionalPropertyEntry(
                STORAGE_POLICY,
                "The name of the storage policy for cold-hot separation",
                false /* immutable */,
                null /* default value */,
                false /* hidden */),
            PropertyEntry.stringReservedPropertyEntry(
                LIGHT_SCHEMA_CHANGE,
                "Whether light schema change is enabled for the table (read-only)",
                false /* hidden */),
            PropertyEntry.stringReservedPropertyEntry(
                ENABLE_UNIQUE_KEY_MERGE_ON_WRITE,
                "Whether merge-on-write is enabled for Unique Key tables (read-only)",
                false /* hidden */));

    PROPERTIES_METADATA = Maps.uniqueIndex(propertyEntries, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
