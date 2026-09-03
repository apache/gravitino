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
import io.airlift.log.Logger;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager.LoadOutcome;

/**
 * An implementation of the load status system table.
 *
 * <p>It reports the health of the loop that registers Apache Gravitino catalogs into Trino. A
 * failure that prevents the loop from listing catalogs at all, such as an unreachable Gravitino
 * server, has no catalog to attach itself to and is only visible here.
 */
public class GravitinoSystemTableLoadStatus extends GravitinoSystemTable {

  private static final Logger LOG = Logger.get(GravitinoSystemTableLoadStatus.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
  private final String metalake;

  /**
   * Constructs a new GravitinoSystemTableLoadStatus.
   *
   * @param catalogConnectorManager the manager for catalog connectors
   * @param metalake the metalake to report errors for
   */
  public GravitinoSystemTableLoadStatus(
      CatalogConnectorManager catalogConnectorManager, String metalake) {
    this.catalogConnectorManager = catalogConnectorManager;
    this.metalake = metalake;
  }

  @Override
  public Page loadPageData() {
    BlockBuilder trinoStartedColumnBuilder = BOOLEAN.createBlockBuilder(null, 1);
    BlockBuilder lastAttemptTimeColumnBuilder = VARCHAR.createBlockBuilder(null, 1);
    BlockBuilder lastSuccessTimeColumnBuilder = VARCHAR.createBlockBuilder(null, 1);
    BlockBuilder consecutiveFailuresColumnBuilder = BIGINT.createBlockBuilder(null, 1);
    BlockBuilder lastErrorColumnBuilder = VARCHAR.createBlockBuilder(null, 1);
    BlockBuilder metalakeErrorsColumnBuilder = VARCHAR.createBlockBuilder(null, 1);

    // Read once so that every field of the row below comes from the same attempt: reading them
    // through separate calls could otherwise mix a fresh success time with a stale error from a
    // load that completed in between the calls.
    LoadOutcome loadOutcome = catalogConnectorManager.getLoadOutcome();

    BOOLEAN.writeBoolean(trinoStartedColumnBuilder, loadOutcome.isTrinoStarted());
    writeTime(lastAttemptTimeColumnBuilder, catalogConnectorManager.getLastLoadAttemptTimeMs());
    writeTime(lastSuccessTimeColumnBuilder, loadOutcome.getLastSuccessTimeMs());
    BIGINT.writeLong(consecutiveFailuresColumnBuilder, loadOutcome.getConsecutiveFailures());
    writeNullableString(lastErrorColumnBuilder, loadOutcome.getLastError());

    // The load loop itself is shared by every entry catalog, so the columns above are global.
    // Only the per metalake errors are narrowed to the metalake this connector reports on.
    Map<String, String> allErrors = loadOutcome.getMetalakeErrors();
    Map<String, String> metalakeErrors =
        allErrors.containsKey(metalake) ? Map.of(metalake, allErrors.get(metalake)) : Map.of();
    if (metalakeErrors.isEmpty()) {
      metalakeErrorsColumnBuilder.appendNull();
    } else {
      try {
        VARCHAR.writeString(
            metalakeErrorsColumnBuilder,
            OBJECT_MAPPER.writeValueAsString(new TreeMap<>(metalakeErrors)));
      } catch (JsonProcessingException e) {
        // Degrade rather than fail: this table is what a user reads while diagnosing a broken
        // load loop, and one unserializable column must not take last_error down with it.
        LOG.warn(e, "Failed to serialize the metalake errors");
        VARCHAR.writeString(metalakeErrorsColumnBuilder, new TreeMap<>(metalakeErrors).toString());
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
