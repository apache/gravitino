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
package org.apache.gravitino.catalog.hive;

import static org.apache.gravitino.catalog.hive.HiveConstants.COMMENT;
import static org.apache.gravitino.catalog.hive.HiveConstants.EXTERNAL;
import static org.apache.gravitino.catalog.hive.HiveConstants.FORMAT;
import static org.apache.gravitino.catalog.hive.HiveConstants.INPUT_FORMAT;
import static org.apache.gravitino.catalog.hive.HiveConstants.LOCATION;
import static org.apache.gravitino.catalog.hive.HiveConstants.NUM_FILES;
import static org.apache.gravitino.catalog.hive.HiveConstants.OUTPUT_FORMAT;
import static org.apache.gravitino.catalog.hive.HiveConstants.SERDE_LIB;
import static org.apache.gravitino.catalog.hive.HiveConstants.SERDE_NAME;
import static org.apache.gravitino.catalog.hive.HiveConstants.TABLE_TYPE;
import static org.apache.gravitino.catalog.hive.HiveConstants.TOTAL_SIZE;
import static org.apache.gravitino.catalog.hive.HiveConstants.TRANSIENT_LAST_DDL_TIME;
import static org.apache.gravitino.connector.PropertyEntry.booleanReservedPropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.enumImmutablePropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringImmutablePropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringReservedPropertyEntry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

public class HiveTablePropertiesMetadata extends BasePropertiesMetadata {

  private static final Map<String, PropertyEntry<?>> propertiesMetadata;

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            stringReservedPropertyEntry(COMMENT, "table comment", true),
            stringReservedPropertyEntry(NUM_FILES, "number of files", false),
            stringReservedPropertyEntry(TOTAL_SIZE, "total size of the table", false),
            booleanReservedPropertyEntry(
                EXTERNAL, "Indicate whether it is an external table", false, true),
            stringImmutablePropertyEntry(
                LOCATION,
                "The location for table storage. Not required, HMS will use the database location as the parent directory by default",
                false,
                null,
                false,
                false),
            enumImmutablePropertyEntry(
                TABLE_TYPE,
                "Type of the table",
                false,
                TableType.class,
                TableType.MANAGED_TABLE,
                false,
                false),
            enumImmutablePropertyEntry(
                FORMAT,
                "The table file format",
                false,
                StorageFormat.class,
                StorageFormat.TEXTFILE,
                false,
                false),
            stringImmutablePropertyEntry(
                INPUT_FORMAT,
                "The input format class for the table",
                false,
                HiveStorageConstants.TEXT_INPUT_FORMAT_CLASS,
                false,
                false),
            stringImmutablePropertyEntry(
                OUTPUT_FORMAT,
                "The output format class for the table",
                false,
                HiveStorageConstants.IGNORE_KEY_OUTPUT_FORMAT_CLASS,
                false,
                false),
            stringReservedPropertyEntry(TRANSIENT_LAST_DDL_TIME, "Last DDL time", false),
            stringImmutablePropertyEntry(
                SERDE_NAME, "Name of the serde, table name by default", false, null, false, false),
            stringImmutablePropertyEntry(
                SERDE_LIB,
                "The serde library class for the table",
                false,
                HiveStorageConstants.LAZY_SIMPLE_SERDE_CLASS,
                false,
                false));

    propertiesMetadata = Maps.uniqueIndex(propertyEntries, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return propertiesMetadata;
  }
}
