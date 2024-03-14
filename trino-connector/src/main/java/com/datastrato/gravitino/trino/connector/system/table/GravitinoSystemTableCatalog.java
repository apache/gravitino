/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system.table;

import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.type.VarcharType.VARCHAR;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import com.google.common.base.Preconditions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import java.util.List;

/** An implementation of the catalog system table * */
public class GravitinoSystemTableCatalog extends GravitinoSystemTable {

  public static final SchemaTableName TABLE_NAME =
      new SchemaTableName(SYSTEM_TABLE_SCHEMA_NAME, "catalog");

  private static final MapType STRING_MAPTYPE = new MapType(VARCHAR, VARCHAR, new TypeOperators());

  private static final ConnectorTableMetadata TABLE_METADATA =
      new ConnectorTableMetadata(
          TABLE_NAME,
          List.of(
              ColumnMetadata.builder().setName("name").setType(VARCHAR).build(),
              ColumnMetadata.builder().setName("provider").setType(VARCHAR).build(),
              ColumnMetadata.builder().setName("properties").setType(STRING_MAPTYPE).build()));

  private final CatalogConnectorManager catalogConnectorManager;

  public GravitinoSystemTableCatalog(CatalogConnectorManager catalogConnectorManager) {
    this.catalogConnectorManager = catalogConnectorManager;
  }

  @Override
  public Page loadPageData() {
    int size = catalogConnectorManager.getCatalogs().size();

    BlockBuilder nameColumnBuilder = VARCHAR.createBlockBuilder(null, size);
    BlockBuilder providerColumnBuilder = VARCHAR.createBlockBuilder(null, size);
    MapBlockBuilder propertyColumnBuilder = STRING_MAPTYPE.createBlockBuilder(null, size);

    for (GravitinoCatalog catalog : catalogConnectorManager.getCatalogs()) {
      Preconditions.checkNotNull(catalog, "catalog not be null");

      VARCHAR.writeString(nameColumnBuilder, catalog.getFullName());
      VARCHAR.writeString(providerColumnBuilder, catalog.getProvider());
      Block mapValue =
          buildMapValue(
              STRING_MAPTYPE,
              catalog.getProperties().size(),
              (keyBuilder, valueBuilder) ->
                  catalog
                      .getProperties()
                      .forEach(
                          (key, value) -> {
                            VARCHAR.writeString(keyBuilder, key);
                            VARCHAR.writeString(valueBuilder, value);
                          }));
      STRING_MAPTYPE.writeObject(propertyColumnBuilder, mapValue);
    }
    return new Page(size, nameColumnBuilder, providerColumnBuilder, propertyColumnBuilder);
  }

  @Override
  public ConnectorTableMetadata getTableMetaData() {
    return TABLE_METADATA;
  }
}
