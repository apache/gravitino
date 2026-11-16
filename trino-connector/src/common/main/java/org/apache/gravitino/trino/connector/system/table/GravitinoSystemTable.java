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
package org.apache.gravitino.trino.connector.system.table;

import static io.trino.spi.type.VarcharType.VARCHAR;

import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorTableMetadata;
import java.time.Instant;
import javax.annotation.Nullable;

/** Gravitino System table interfaces */
public abstract class GravitinoSystemTable {

  /** The schema name for system tables */
  public static final String SYSTEM_TABLE_SCHEMA_NAME = "system";

  /**
   * Gets the metadata definition of the system table.
   *
   * @return the connector table metadata containing the table definition
   */
  public abstract ConnectorTableMetadata getTableMetaData();

  /**
   * Loads and returns all data from the system table.
   *
   * @return a Page object containing all the table data
   */
  public abstract Page loadPageData();

  /**
   * Appends a string to a VARCHAR column, writing a null when the value is absent.
   *
   * @param builder the column builder to append to
   * @param value the value to append, may be null
   */
  protected static void writeNullableString(BlockBuilder builder, @Nullable String value) {
    if (value == null) {
      builder.appendNull();
    } else {
      VARCHAR.writeString(builder, value);
    }
  }

  /**
   * Appends a timestamp to a VARCHAR column as an ISO-8601 UTC string, writing a null when the
   * timestamp is unset. Timestamps are rendered as strings because the block encoding of
   * TimestampType differs across the supported Trino versions.
   *
   * @param builder the column builder to append to
   * @param timeMs the time in milliseconds since the epoch, 0 when unset
   */
  protected static void writeTime(BlockBuilder builder, long timeMs) {
    if (timeMs == 0) {
      builder.appendNull();
    } else {
      VARCHAR.writeString(builder, Instant.ofEpochMilli(timeMs).toString());
    }
  }
}
