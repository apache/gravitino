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
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.SchemaTableName;

/**
 * The GravitinoOutputTableHandle is used for handling CTAS (CREATE TABLE AS SELECT) operations.
 *
 * <p>Internally wraps a {@link ConnectorInsertTableHandle} because the Gravitino connector creates
 * the table first via catalogConnectorMetadata, then delegates data writing to the internal
 * connector's insert path, avoiding double table creation.
 *
 * <p>Also stores the {@link SchemaTableName} so that {@code rollbackCreateTable} can drop the table
 * from the Gravitino catalog if the CTAS operation fails.
 */
public class GravitinoOutputTableHandle
    implements ConnectorOutputTableHandle, GravitinoHandle<ConnectorInsertTableHandle> {

  private final String schemaName;
  private final String tableName;

  private HandleWrapper<ConnectorInsertTableHandle> handleWrapper =
      new HandleWrapper<>(ConnectorInsertTableHandle.class);

  /**
   * Constructs a new GravitinoOutputTableHandle from serialized properties.
   *
   * @param handleString the serialized handle string
   * @param schemaName the schema name of the created table
   * @param tableName the table name of the created table
   */
  @JsonCreator
  public GravitinoOutputTableHandle(
      @JsonProperty(HANDLE_STRING) String handleString,
      @JsonProperty("schemaName") String schemaName,
      @JsonProperty("tableName") String tableName) {
    this.handleWrapper = handleWrapper.fromJson(handleString);
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  /**
   * Constructs a new GravitinoOutputTableHandle from a ConnectorInsertTableHandle.
   *
   * @param insertTableHandle the internal connector insert table handle
   * @param tableName the schema-qualified table name for rollback support
   */
  public GravitinoOutputTableHandle(
      ConnectorInsertTableHandle insertTableHandle, SchemaTableName tableName) {
    this.handleWrapper = new HandleWrapper<>(insertTableHandle);
    this.schemaName = tableName.getSchemaName();
    this.tableName = tableName.getTableName();
  }

  @JsonProperty
  @Override
  public String getHandleString() {
    return handleWrapper.toJson();
  }

  @Override
  public ConnectorInsertTableHandle getInternalHandle() {
    return handleWrapper.getHandle();
  }

  /** Returns the schema name of the table created during CTAS. */
  @JsonProperty
  public String getSchemaName() {
    return schemaName;
  }

  /** Returns the table name of the table created during CTAS. */
  @JsonProperty
  public String getTableName() {
    return tableName;
  }

  /** Returns the schema-qualified table name for rollback support. */
  public SchemaTableName toSchemaTableName() {
    return new SchemaTableName(schemaName, tableName);
  }
}
