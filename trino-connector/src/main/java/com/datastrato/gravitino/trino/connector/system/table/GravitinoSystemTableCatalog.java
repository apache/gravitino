/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system.table;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import java.util.List;

/** An implementation of the catalog system table */
public class GravitinoSystemTableCatalog extends GravitinoSystemTable {

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

  public GravitinoSystemTableCatalog(CatalogConnectorManager catalogConnectorManager) {
    this.catalogConnectorManager = catalogConnectorManager;
  }

  @Override
  public Page loadPageData() {
    List<GravitinoCatalog> catalogs = catalogConnectorManager.getCatalogs();
    int size = catalogs.size();

    BlockBuilder nameColumnBuilder = VARCHAR.createBlockBuilder(null, size);
    BlockBuilder providerColumnBuilder = VARCHAR.createBlockBuilder(null, size);
    BlockBuilder propertyColumnBuilder = VARCHAR.createBlockBuilder(null, size);

    for (GravitinoCatalog catalog : catalogs) {
      Preconditions.checkNotNull(catalog, "catalog should not be null");

      VARCHAR.writeString(nameColumnBuilder, catalog.getFullName());
      VARCHAR.writeString(providerColumnBuilder, catalog.getProvider());
      try {
        VARCHAR.writeString(
            propertyColumnBuilder, new ObjectMapper().writeValueAsString(catalog.getProperties()));
      } catch (JsonProcessingException e) {
        throw new TrinoException(GRAVITINO_ILLEGAL_ARGUMENT, "Invalid property format", e); //
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
