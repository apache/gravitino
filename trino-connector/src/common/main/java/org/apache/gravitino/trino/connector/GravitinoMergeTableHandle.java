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
package org.apache.gravitino.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;

/** The GravitinoMergeTableHandle is used for handling merge operations. */
public class GravitinoMergeTableHandle
    implements ConnectorMergeTableHandle, GravitinoHandle<ConnectorMergeTableHandle> {

  private final String schemaName;
  private final String tableName;

  private HandleWrapper<ConnectorMergeTableHandle> handleWrapper =
      new HandleWrapper<>(ConnectorMergeTableHandle.class);

  /**
   * Constructs a new ConnectorMergeTableHandle from a serialized schema name, table name, and
   * handle string.
   *
   * @param schemaName the name of the schema
   * @param tableName the name of the table
   * @param handleString the serialized handle string
   */
  @JsonCreator
  public GravitinoMergeTableHandle(
      @JsonProperty("schemaName") String schemaName,
      @JsonProperty("tableName") String tableName,
      @JsonProperty(HANDLE_STRING) String handleString) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.handleWrapper = handleWrapper.fromJson(handleString);
  }

  /**
   * Constructs a new GravitinoMergeTableHandle from a ConnectorMergeTableHandle.
   *
   * @param mergeTableHandle the internal connector merge table handle
   */
  public GravitinoMergeTableHandle(
      String schemaName, String tableName, ConnectorMergeTableHandle mergeTableHandle) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.handleWrapper = new HandleWrapper<>(mergeTableHandle);
  }

  /**
   * Gets the name of the schema.
   *
   * @return the schema name
   */
  @JsonProperty
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * Gets the name of the table.
   *
   * @return the table name
   */
  @JsonProperty
  public String getTableName() {
    return tableName;
  }

  @JsonProperty
  @Override
  public String getHandleString() {
    return handleWrapper.toJson();
  }

  @Override
  public ConnectorMergeTableHandle getInternalHandle() {
    return handleWrapper.getHandle();
  }

  @Override
  public ConnectorTableHandle getTableHandle() {
    return new GravitinoTableHandle(
        schemaName, tableName, handleWrapper.getHandle().getTableHandle());
  }
}
