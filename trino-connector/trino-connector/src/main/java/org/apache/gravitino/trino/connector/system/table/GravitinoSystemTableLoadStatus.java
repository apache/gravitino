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

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;

/**
 * An implementation of the load status system table.
 *
 * <p>It reports the health of the loop that registers Apache Gravitino catalogs into Trino. A
 * failure that prevents the loop from listing catalogs at all, such as an unreachable Gravitino
 * server, has no catalog to attach itself to and is only visible here.
 */
public class GravitinoSystemTableLoadStatus extends GravitinoSystemTable {

  /** The name of the load status system table. */
  public static final SchemaTableName TABLE_NAME =
      new SchemaTableName(SYSTEM_TABLE_SCHEMA_NAME, "load_status");

  private static final ConnectorTableMetadata TABLE_METADATA =
      new ConnectorTableMetadata(
          TABLE_NAME,
          List.of(
              ColumnMetadata.builder().setName("trino_started").setType(BOOLEAN).build(),
              ColumnMetadata.builder().setName("last_attempt_time").setType(VARCHAR).build(),
              ColumnMetadata.builder().setName("last_success_time").setType(VARCHAR).build(),
              ColumnMetadata.builder().setName("consecutive_failures").setType(BIGINT).build(),
              ColumnMetadata.builder().setName("last_error").setType(VARCHAR).build(),
              ColumnMetadata.builder().setName("metalake_errors").setType(VARCHAR).build()));

  private final CatalogConnectorManager catalogConnectorManager;

  /**
   * Constructs a new GravitinoSystemTableLoadStatus.
   *
   * @param catalogConnectorManager the manager for catalog connectors
   */
  public GravitinoSystemTableLoadStatus(CatalogConnectorManager catalogConnectorManager) {
    this.catalogConnectorManager = catalogConnectorManager;
  }

  @Override
  public Page loadPageData() {
    BlockBuilder trinoStartedColumnBuilder = BOOLEAN.createBlockBuilder(null, 1);
    BlockBuilder lastAttemptTimeColumnBuilder = VARCHAR.createBlockBuilder(null, 1);
    BlockBuilder lastSuccessTimeColumnBuilder = VARCHAR.createBlockBuilder(null, 1);
    BlockBuilder consecutiveFailuresColumnBuilder = BIGINT.createBlockBuilder(null, 1);
    BlockBuilder lastErrorColumnBuilder = VARCHAR.createBlockBuilder(null, 1);
    BlockBuilder metalakeErrorsColumnBuilder = VARCHAR.createBlockBuilder(null, 1);

    BOOLEAN.writeBoolean(trinoStartedColumnBuilder, catalogConnectorManager.isTrinoStarted());
    writeTime(lastAttemptTimeColumnBuilder, catalogConnectorManager.getLastLoadAttemptTimeMs());
    writeTime(lastSuccessTimeColumnBuilder, catalogConnectorManager.getLastSuccessfulLoadTimeMs());
    BIGINT.writeLong(
        consecutiveFailuresColumnBuilder, catalogConnectorManager.getConsecutiveLoadFailures());
    writeNullableString(lastErrorColumnBuilder, catalogConnectorManager.getLastLoadError());

    Map<String, String> metalakeErrors = catalogConnectorManager.getMetalakeErrors();
    if (metalakeErrors.isEmpty()) {
      metalakeErrorsColumnBuilder.appendNull();
    } else {
      try {
        VARCHAR.writeString(
            metalakeErrorsColumnBuilder,
            new ObjectMapper().writeValueAsString(new TreeMap<>(metalakeErrors)));
      } catch (JsonProcessingException e) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT, "Invalid metalake error format", e);
      }
    }

    return new Page(
        1,
        trinoStartedColumnBuilder.build(),
        lastAttemptTimeColumnBuilder.build(),
        lastSuccessTimeColumnBuilder.build(),
        consecutiveFailuresColumnBuilder.build(),
        lastErrorColumnBuilder.build(),
        metalakeErrorsColumnBuilder.build());
  }

  @Override
  public ConnectorTableMetadata getTableMetaData() {
    return TABLE_METADATA;
  }
}
