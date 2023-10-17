/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_CATALOG_NOT_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_SCHEMA_ALREADY_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_SCHEMA_NOT_EMPTY;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_SCHEMA_NOT_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_TABLE_ALREADY_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_TABLE_NOT_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoSchema;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoTable;
import io.trino.spi.TrinoException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class implements gravitino metadata operators. */
public class CatalogConnectorMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogConnectorMetadata.class);

  private final GravitinoMetaLake metalake;
  private final String catalogName;
  private final SupportsSchemas schemaCatalog;
  private final TableCatalog tableCatalog;

  public CatalogConnectorMetadata(GravitinoMetaLake metalake, NameIdentifier catalogIdentifier) {
    try {
      this.catalogName = catalogIdentifier.name();
      this.metalake = metalake;
      Catalog catalog = metalake.loadCatalog(catalogIdentifier);

      // Make sure the catalog support schema operations.
      this.schemaCatalog = catalog.asSchemas();
      this.tableCatalog = catalog.asTableCatalog();
    } catch (NoSuchCatalogException e) {
      throw new TrinoException(GRAVITINO_CATALOG_NOT_EXISTS, "Catalog does not exist", e);
    } catch (UnsupportedOperationException e) {
      throw new TrinoException(
          GRAVITINO_UNSUPPORTED_OPERATION,
          "Catalog does not support schema or table operations",
          e);
    }
  }

  public List<String> listSchemaNames() {
    try {
      return Arrays.stream(
              schemaCatalog.listSchemas(Namespace.ofSchema(metalake.name(), catalogName)))
          .map(NameIdentifier::name)
          .toList();
    } catch (NoSuchCatalogException e) {
      throw new TrinoException(GRAVITINO_CATALOG_NOT_EXISTS, "Catalog does not exist", e);
    }
  }

  public GravitinoSchema getSchema(String schemaName) {
    try {
      Schema schema =
          schemaCatalog.loadSchema(
              NameIdentifier.ofSchema(metalake.name(), catalogName, schemaName));
      return new GravitinoSchema(schema);
    } catch (NoSuchSchemaException e) {
      throw new TrinoException(GRAVITINO_SCHEMA_NOT_EXISTS, "Schema does not exist", e);
    }
  }

  public GravitinoTable getTable(String schemaName, String tableName) {
    try {
      Table table =
          tableCatalog.loadTable(
              NameIdentifier.ofTable(metalake.name(), catalogName, schemaName, tableName));
      return new GravitinoTable(schemaName, tableName, table);
    } catch (NoSuchTableException e) {
      throw new TrinoException(GRAVITINO_TABLE_NOT_EXISTS, "Table does not exist", e);
    }
  }

  public List<String> listTables(String schemaName) {
    try {
      NameIdentifier[] tables =
          tableCatalog.listTables(Namespace.ofTable(metalake.name(), catalogName, schemaName));
      return Arrays.stream(tables).map(NameIdentifier::name).toList();
    } catch (NoSuchSchemaException e) {
      throw new TrinoException(GRAVITINO_SCHEMA_NOT_EXISTS, "Schema does not exist", e);
    }
  }

  public boolean tableExists(String schemaName, String tableName) {
    return tableCatalog.tableExists(
        NameIdentifier.ofTable(metalake.name(), catalogName, schemaName, tableName));
  }

  public void createTable(GravitinoTable table) {
    NameIdentifier identifier =
        NameIdentifier.ofTable(
            metalake.name(), catalogName, table.getSchemaName(), table.getName());
    ColumnDTO[] gravitinoColumns = table.getColumnDTOs();
    String comment = table.getComment();
    Map<String, String> properties = table.getProperties();
    try {
      tableCatalog.createTable(identifier, gravitinoColumns, comment, properties);
    } catch (NoSuchSchemaException e) {
      throw new TrinoException(GRAVITINO_SCHEMA_NOT_EXISTS, "Schema does not exist", e);
    } catch (TableAlreadyExistsException e) {
      throw new TrinoException(GRAVITINO_TABLE_ALREADY_EXISTS, "Table already exists", e);
    }
  }

  public void createSchema(GravitinoSchema schema) {
    try {
      schemaCatalog.createSchema(
          NameIdentifier.ofSchema(metalake.name(), catalogName, schema.getName()),
          schema.getComment(),
          schema.getProperties());
    } catch (NoSuchSchemaException e) {
      throw new TrinoException(GRAVITINO_CATALOG_NOT_EXISTS, "Catalog does not exist", e);
    } catch (TableAlreadyExistsException e) {
      throw new TrinoException(GRAVITINO_SCHEMA_ALREADY_EXISTS, "Schema already exists", e);
    }
  }

  public void dropSchema(String schemaName, boolean cascade) {
    try {
      schemaCatalog.dropSchema(
          NameIdentifier.ofSchema(metalake.name(), catalogName, schemaName), cascade);
    } catch (NonEmptySchemaException e) {
      throw new TrinoException(GRAVITINO_SCHEMA_NOT_EMPTY, "Schema does not empty", e);
    }
  }

  public void dropTable(String schemaName, String tableName) {
    boolean dropped =
        tableCatalog.dropTable(
            NameIdentifier.ofTable(metalake.name(), catalogName, schemaName, tableName));
    if (!dropped) throw new TrinoException(GRAVITINO_TABLE_NOT_EXISTS, "Table does not exist");
  }
}
