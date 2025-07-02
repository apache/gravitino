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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;

/** An implementation of the catalog system table */
public class GravitinoSystemTableCatalog extends GravitinoSystemTable {

  /** The name of the catalog system table. */
  public static final SchemaTableName TABLE_NAME =
      new SchemaTableName(SYSTEM_TABLE_SCHEMA_NAME, "catalog");

  private static final ConnectorTableMetadata TABLE_METADATA =
      new ConnectorTableMetadata(
          TABLE_NAME,
          List.of(
              ColumnMetadata.builder().setName("name").setType(VARCHAR).build(),
              ColumnMetadata.builder().setName("provider").setType(VARCHAR).build(),
              ColumnMetadata.builder().setName("properties").setType(VARCHAR).build()));

  private final CatalogConnectorManager catalogConnectorManager;

  /**
   * Constructs a new GravitinoSystemTableCatalog.
   *
   * @param catalogConnectorManager the manager for catalog connectors
   */
  public GravitinoSystemTableCatalog(CatalogConnectorManager catalogConnectorManager) {
    this.catalogConnectorManager = catalogConnectorManager;
  }

  @Override
  public Page loadPageData() {
    List<GravitinoCatalog> gravitinoCatalogs = new ArrayList<>();
    // retrieve catalogs form the Gravitino server with the configuration metalakes,
    // the catalogConnectorManager does not manager catalogs in worker nodes
    catalogConnectorManager
        .getUsedMetalakes()
        .forEach(
            (metalakeName) -> {
              GravitinoMetalake metalake = catalogConnectorManager.getMetalake(metalakeName);
              Catalog[] catalogs = metalake.listCatalogsInfo();
              for (Catalog catalog : catalogs) {
                if (catalog.type() == Catalog.Type.RELATIONAL) {
                  gravitinoCatalogs.add(new GravitinoCatalog(metalakeName, catalog));
                }
              }
            });
    int size = gravitinoCatalogs.size();

    BlockBuilder nameColumnBuilder = VARCHAR.createBlockBuilder(null, size);
    BlockBuilder providerColumnBuilder = VARCHAR.createBlockBuilder(null, size);
    BlockBuilder propertyColumnBuilder = VARCHAR.createBlockBuilder(null, size);

    for (GravitinoCatalog catalog : gravitinoCatalogs) {
      Preconditions.checkNotNull(catalog, "catalog should not be null");

      VARCHAR.writeString(nameColumnBuilder, catalog.getName());
      VARCHAR.writeString(providerColumnBuilder, catalog.getProvider());
      try {
        VARCHAR.writeString(
            propertyColumnBuilder,
            new ObjectMapper().writeValueAsString(new TreeMap<>(catalog.getProperties())));
      } catch (JsonProcessingException e) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT, "Invalid property format", e); //
      }
    }
    return new Page(
        size,
        nameColumnBuilder.build(),
        providerColumnBuilder.build(),
        propertyColumnBuilder.build());
  }

  @Override
  public ConnectorTableMetadata getTableMetaData() {
    return TABLE_METADATA;
  }
}
