/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.catalog;

import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_CATALOG_NOT_EXISTS;
import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_SCHEMA_NOT_EXISTS;
import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_TABLE_NOT_EXISTS;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.client.GravitonMetaLake;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchSchemaException;
import com.datastrato.graviton.exceptions.NoSuchTableException;
import com.datastrato.graviton.rel.Schema;
import com.datastrato.graviton.rel.SupportsSchemas;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.TableCatalog;
import com.datastrato.graviton.trino.connector.metadata.GravitonSchema;
import com.datastrato.graviton.trino.connector.metadata.GravitonTable;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import java.util.Arrays;
import java.util.List;

/** This class implements graviton metadata operators. */
public class CatalogConnectorMetadata {

  private static final Logger LOG = Logger.get(CatalogConnectorMetadata.class);

  private final GravitonMetaLake metalake;
  private final Catalog catalog;
  private final String catalogName;

  public CatalogConnectorMetadata(GravitonMetaLake metalake, NameIdentifier catalogIdentifier) {
    try {
      this.catalogName = catalogIdentifier.name();
      this.metalake = metalake;
      catalog = metalake.loadCatalog(catalogIdentifier);
    } catch (NoSuchCatalogException e) {
      throw new TrinoException(GRAVITON_CATALOG_NOT_EXISTS, "Catalog does not exist", e);
    }
  }

  public List<String> listSchemaNames() {
    try {
      SupportsSchemas schemas = catalog.asSchemas();
      return Arrays.stream(schemas.listSchemas(Namespace.ofSchema(metalake.name(), catalogName)))
          .map(NameIdentifier::name)
          .toList();
    } catch (NoSuchCatalogException e) {
      throw new TrinoException(GRAVITON_CATALOG_NOT_EXISTS, "Catalog does not exist", e);
    }
  }

  public GravitonSchema getSchema(String schemaName) {
    try {
      SupportsSchemas schemaCatalog = catalog.asSchemas();
      Schema schema =
          schemaCatalog.loadSchema(
              NameIdentifier.ofSchema(metalake.name(), catalogName, schemaName));
      return new GravitonSchema(schema);
    } catch (NoSuchSchemaException e) {
      throw new TrinoException(GRAVITON_SCHEMA_NOT_EXISTS, "Schema does not exist", e);
    }
  }

  public GravitonTable getTable(String schemaName, String tableName) {
    try {
      TableCatalog tableCatalog = catalog.asTableCatalog();
      Table table =
          tableCatalog.loadTable(
              NameIdentifier.ofTable(metalake.name(), catalogName, schemaName, tableName));
      return new GravitonTable(schemaName, table);
    } catch (NoSuchTableException e) {
      throw new TrinoException(GRAVITON_TABLE_NOT_EXISTS, "Table does not exist", e);
    }
  }

  public List<String> listTables(String schemaName) {
    try {
      TableCatalog tableCatalog = catalog.asTableCatalog();
      NameIdentifier[] tables =
          tableCatalog.listTables(Namespace.ofTable(metalake.name(), catalogName, schemaName));
      return Arrays.stream(tables).map(NameIdentifier::name).toList();
    } catch (NoSuchSchemaException e) {
      throw new TrinoException(GRAVITON_SCHEMA_NOT_EXISTS, "Schema does not exist", e);
    }
  }

  public boolean tableExists(String schemaName, String tableName) {
    return catalog
        .asTableCatalog()
        .tableExists(NameIdentifier.ofTable(metalake.name(), catalogName, schemaName, tableName));
  }
}
