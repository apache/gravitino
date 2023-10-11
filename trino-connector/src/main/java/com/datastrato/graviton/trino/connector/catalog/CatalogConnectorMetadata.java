/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.catalog;

import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_CATALOG_NOT_EXISTS;
import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_SCHEMA_NOT_EXISTS;
import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_TABLE_NOT_EXISTS;
import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_UNSUPPORTED_OPERATION;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.client.GravitonMetaLake;
import com.datastrato.graviton.dto.rel.ColumnDTO;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchSchemaException;
import com.datastrato.graviton.exceptions.NoSuchTableException;
import com.datastrato.graviton.rel.Schema;
import com.datastrato.graviton.rel.SupportsSchemas;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.TableCatalog;
import com.datastrato.graviton.trino.connector.metadata.GravitonSchema;
import com.datastrato.graviton.trino.connector.metadata.GravitonTable;
import io.trino.spi.TrinoException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class implements graviton metadata operators. */
public class CatalogConnectorMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogConnectorMetadata.class);

  private final GravitonMetaLake metalake;
  private final String catalogName;
  private final SupportsSchemas schemaCatalog;
  private final TableCatalog tableCatalog;

  public CatalogConnectorMetadata(GravitonMetaLake metalake, NameIdentifier catalogIdentifier) {
    try {
      this.catalogName = catalogIdentifier.name();
      this.metalake = metalake;
      Catalog catalog = metalake.loadCatalog(catalogIdentifier);

      // Make sure the catalog support schema operations.
      this.schemaCatalog = catalog.asSchemas();
      this.tableCatalog = catalog.asTableCatalog();
    } catch (NoSuchCatalogException e) {
      throw new TrinoException(GRAVITON_CATALOG_NOT_EXISTS, "Catalog does not exist", e);
    } catch (UnsupportedOperationException e) {
      throw new TrinoException(
          GRAVITON_UNSUPPORTED_OPERATION, "Catalog does not support schema or table operations", e);
    }
  }

  public List<String> listSchemaNames() {
    try {
      return Arrays.stream(
              schemaCatalog.listSchemas(Namespace.ofSchema(metalake.name(), catalogName)))
          .map(NameIdentifier::name)
          .toList();
    } catch (NoSuchCatalogException e) {
      throw new TrinoException(GRAVITON_CATALOG_NOT_EXISTS, "Catalog does not exist", e);
    }
  }

  public GravitonSchema getSchema(String schemaName) {
    try {
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
      Table table =
          tableCatalog.loadTable(
              NameIdentifier.ofTable(metalake.name(), catalogName, schemaName, tableName));
      return new GravitonTable(schemaName, tableName, table);
    } catch (NoSuchTableException e) {
      throw new TrinoException(GRAVITON_TABLE_NOT_EXISTS, "Table does not exist", e);
    }
  }

  public List<String> listTables(String schemaName) {
    try {
      NameIdentifier[] tables =
          tableCatalog.listTables(Namespace.ofTable(metalake.name(), catalogName, schemaName));
      return Arrays.stream(tables).map(NameIdentifier::name).toList();
    } catch (NoSuchSchemaException e) {
      throw new TrinoException(GRAVITON_SCHEMA_NOT_EXISTS, "Schema does not exist", e);
    }
  }

  public boolean tableExists(String schemaName, String tableName) {
    return tableCatalog.tableExists(
        NameIdentifier.ofTable(metalake.name(), catalogName, schemaName, tableName));
  }

  public void createTable(GravitonTable table) {
    NameIdentifier identifier =
        NameIdentifier.ofTable(
            metalake.name(), catalogName, table.getSchemaName(), table.getName());
    ColumnDTO[] gravitonColumns = table.getColumnDTOs();
    String comment = table.getComment();
    Map<String, String> properties = table.getProperties();
    tableCatalog.createTable(identifier, gravitonColumns, comment, properties);
  }

  public void createSchema(GravitonSchema schema) {
    schemaCatalog.createSchema(
        NameIdentifier.ofSchema(metalake.name(), catalogName, schema.getName()),
        schema.getComment(),
        schema.getProperties());
  }

  public void dropSchema(String schemaName, boolean cascade) {
    schemaCatalog.dropSchema(
        NameIdentifier.ofSchema(metalake.name(), catalogName, schemaName), cascade);
  }

  public void dropTable(String schemaName, String tableName) {
    tableCatalog.dropTable(
        NameIdentifier.ofTable(metalake.name(), catalogName, schemaName, tableName));
  }
}
