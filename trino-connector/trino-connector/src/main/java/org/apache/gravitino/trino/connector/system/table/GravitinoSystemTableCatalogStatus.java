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
import static io.trino.spi.type.VarcharType.VARCHAR;

import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import java.util.List;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;
import org.apache.gravitino.trino.connector.catalog.CatalogRegistrationState;

/**
 * An implementation of the catalog status system table.
 *
 * <p>It reports why every Apache Gravitino catalog is or is not registered in Trino, so that a
 * catalog missing from SHOW CATALOGS can be diagnosed without reading the coordinator log.
 */
public class GravitinoSystemTableCatalogStatus extends GravitinoSystemTable {

  /** The name of the catalog status system table. */
  public static final SchemaTableName TABLE_NAME =
      new SchemaTableName(SYSTEM_TABLE_SCHEMA_NAME, "catalog_status");

  private static final ConnectorTableMetadata TABLE_METADATA =
      new ConnectorTableMetadata(
          TABLE_NAME,
          List.of(
              ColumnMetadata.builder().setName("metalake").setType(VARCHAR).build(),
              ColumnMetadata.builder().setName("catalog_name").setType(VARCHAR).build(),
              ColumnMetadata.builder().setName("trino_catalog_name").setType(VARCHAR).build(),
              ColumnMetadata.builder().setName("provider").setType(VARCHAR).build(),
              ColumnMetadata.builder().setName("status").setType(VARCHAR).build(),
              ColumnMetadata.builder().setName("last_error").setType(VARCHAR).build(),
              ColumnMetadata.builder().setName("last_attempt_time").setType(VARCHAR).build(),
              ColumnMetadata.builder().setName("last_success_time").setType(VARCHAR).build(),
              ColumnMetadata.builder().setName("failure_count").setType(BIGINT).build()));

  private final CatalogConnectorManager catalogConnectorManager;
  private final String metalake;

  /**
   * Constructs a new GravitinoSystemTableCatalogStatus.
   *
   * @param catalogConnectorManager the manager for catalog connectors
   * @param metalake the metalake to report on
   */
  public GravitinoSystemTableCatalogStatus(
      CatalogConnectorManager catalogConnectorManager, String metalake) {
    this.catalogConnectorManager = catalogConnectorManager;
    this.metalake = metalake;
  }

  @Override
  public Page loadPageData() {
    // Take a snapshot first, the load loop writes these states concurrently and the column
    // builders must all end up with the same number of positions.
    // The load loop is shared by every entry catalog in this Trino, so report only the metalake
    // this connector is configured with.
    List<CatalogRegistrationState> states =
        catalogConnectorManager.getCatalogRegistrationStates().stream()
            .filter(state -> state.getMetalake().equals(metalake))
            .toList();
    int size = states.size();

    BlockBuilder metalakeColumnBuilder = VARCHAR.createBlockBuilder(null, size);
    BlockBuilder catalogNameColumnBuilder = VARCHAR.createBlockBuilder(null, size);
    BlockBuilder trinoCatalogNameColumnBuilder = VARCHAR.createBlockBuilder(null, size);
    BlockBuilder providerColumnBuilder = VARCHAR.createBlockBuilder(null, size);
    BlockBuilder statusColumnBuilder = VARCHAR.createBlockBuilder(null, size);
    BlockBuilder lastErrorColumnBuilder = VARCHAR.createBlockBuilder(null, size);
    BlockBuilder lastAttemptTimeColumnBuilder = VARCHAR.createBlockBuilder(null, size);
    BlockBuilder lastSuccessTimeColumnBuilder = VARCHAR.createBlockBuilder(null, size);
    BlockBuilder failureCountColumnBuilder = BIGINT.createBlockBuilder(null, size);

    for (CatalogRegistrationState state : states) {
      VARCHAR.writeString(metalakeColumnBuilder, state.getMetalake());
      VARCHAR.writeString(catalogNameColumnBuilder, state.getCatalogName());
      VARCHAR.writeString(trinoCatalogNameColumnBuilder, state.getTrinoCatalogName());
      writeNullableString(providerColumnBuilder, state.getProvider());
      VARCHAR.writeString(statusColumnBuilder, state.getStatus().name());
      writeNullableString(lastErrorColumnBuilder, state.getLastError());
      writeTime(lastAttemptTimeColumnBuilder, state.getLastAttemptTimeMs());
      writeTime(lastSuccessTimeColumnBuilder, state.getLastSuccessTimeMs());
      BIGINT.writeLong(failureCountColumnBuilder, state.getFailureCount());
    }

    return new Page(
        size,
        metalakeColumnBuilder.build(),
        catalogNameColumnBuilder.build(),
        trinoCatalogNameColumnBuilder.build(),
        providerColumnBuilder.build(),
        statusColumnBuilder.build(),
        lastErrorColumnBuilder.build(),
        lastAttemptTimeColumnBuilder.build(),
        lastSuccessTimeColumnBuilder.build(),
        failureCountColumnBuilder.build());
  }

  @Override
  public ConnectorTableMetadata getTableMetaData() {
    return TABLE_METADATA;
  }
}
