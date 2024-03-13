/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_CATALOG_NOT_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_OPERATION_FAILED;
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
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoColumn;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoSchema;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoTable;
import com.google.common.base.Strings;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;

/** This class implements gravitino metadata operators. */
public class CatalogConnectorMetadata {

  private static final String CATALOG_DOES_NOT_EXIST_MSG = "Catalog does not exist";
  private static final String SCHEMA_DOES_NOT_EXIST_MSG = "Schema does not exist";

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
      throw new TrinoException(GRAVITINO_CATALOG_NOT_EXISTS, CATALOG_DOES_NOT_EXIST_MSG, e);
    } catch (UnsupportedOperationException e) {
      throw new TrinoException(
          GRAVITINO_UNSUPPORTED_OPERATION,
          "Catalog does not support schema or table operations." + e.getMessage(),
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
      throw new TrinoException(GRAVITINO_CATALOG_NOT_EXISTS, CATALOG_DOES_NOT_EXIST_MSG, e);
    }
  }

  public GravitinoSchema getSchema(String schemaName) {
    try {
      Schema schema =
          schemaCatalog.loadSchema(
              NameIdentifier.ofSchema(metalake.name(), catalogName, schemaName));
      return new GravitinoSchema(schema);
    } catch (NoSuchSchemaException e) {
      throw new TrinoException(GRAVITINO_SCHEMA_NOT_EXISTS, SCHEMA_DOES_NOT_EXIST_MSG, e);
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
      throw new TrinoException(GRAVITINO_SCHEMA_NOT_EXISTS, SCHEMA_DOES_NOT_EXIST_MSG, e);
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
    try {
      tableCatalog.createTable(
          identifier,
          table.getRawColumns(),
          table.getComment(),
          table.getProperties(),
          table.getPartitioning(),
          table.getDistribution(),
          table.getSortOrders());
    } catch (NoSuchSchemaException e) {
      throw new TrinoException(GRAVITINO_SCHEMA_NOT_EXISTS, SCHEMA_DOES_NOT_EXIST_MSG, e);
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
      throw new TrinoException(GRAVITINO_CATALOG_NOT_EXISTS, CATALOG_DOES_NOT_EXIST_MSG, e);
    } catch (TableAlreadyExistsException e) {
      throw new TrinoException(GRAVITINO_SCHEMA_ALREADY_EXISTS, "Schema already exists", e);
    }
  }

  public void dropSchema(String schemaName, boolean cascade) {
    try {
      boolean success =
          schemaCatalog.dropSchema(
              NameIdentifier.ofSchema(metalake.name(), catalogName, schemaName), cascade);

      if (!success) {
        throw new TrinoException(GRAVITINO_SCHEMA_NOT_EXISTS, "Drop schema failed");
      }

    } catch (NonEmptySchemaException e) {
      throw new TrinoException(GRAVITINO_SCHEMA_NOT_EMPTY, "Schema does not empty", e);
    }
  }

  public void dropTable(SchemaTableName tableName) {
    boolean dropped =
        tableCatalog.dropTable(
            NameIdentifier.ofTable(
                metalake.name(), catalogName, tableName.getSchemaName(), tableName.getTableName()));
    if (!dropped) {
      throw new TrinoException(GRAVITINO_OPERATION_FAILED, "Failed to drop table " + tableName);
    }
  }

  public void renameSchema(String source, String target) {
    throw new NotImplementedException();
  }

  private void applyAlter(SchemaTableName tableName, TableChange... change) {
    try {
      tableCatalog.alterTable(
          NameIdentifier.ofTable(
              metalake.name(), catalogName, tableName.getSchemaName(), tableName.getTableName()),
          change);
    } catch (NoSuchTableException e) {
      throw new TrinoException(GRAVITINO_TABLE_NOT_EXISTS, "Table does not exist");
    } catch (IllegalArgumentException e) {
      // TODO yuhui need improve get the error message. From IllegalArgumentException.
      // At present, the IllegalArgumentException cannot get the error information clearly from the
      // Gravitino server.
      String message =
          e.getMessage().lines().toList().get(0) + e.getMessage().lines().toList().get(1);
      throw new TrinoException(GRAVITINO_ILLEGAL_ARGUMENT, message, e);
    }
  }

  public void renameTable(SchemaTableName oldTableName, SchemaTableName newTableName) {
    if (!oldTableName.getSchemaName().equals(newTableName.getSchemaName())) {
      throw new TrinoException(
          GRAVITINO_UNSUPPORTED_OPERATION, "Cannot rename table across schemas");
    }
    if (oldTableName.getTableName().equals(newTableName.getTableName())) {
      return;
    }
    applyAlter(oldTableName, TableChange.rename(newTableName.getTableName()));
  }

  public void setTableComment(SchemaTableName schemaTableName, String comment) {
    applyAlter(schemaTableName, TableChange.updateComment(comment));
  }

  public void setTableProperties(SchemaTableName schemaTableName, Map<String, String> properties) {
    Map<String, String> oldProperties =
        getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName()).getProperties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (!entry.getValue().equals(oldProperties.get(entry.getKey()))) {
        applyAlter(schemaTableName, TableChange.setProperty(entry.getKey(), entry.getValue()));
      }
    }
  }

  public void addColumn(SchemaTableName schemaTableName, GravitinoColumn column) {
    String[] columnNames = {column.getName()};
    if (Strings.isNullOrEmpty(column.getComment()))
      applyAlter(
          schemaTableName,
          TableChange.addColumn(columnNames, column.getType(), column.isNullable()));
    else {
      applyAlter(
          schemaTableName,
          TableChange.addColumn(
              columnNames, column.getType(), column.getComment(), column.isNullable()));
    }
  }

  public void dropColumn(SchemaTableName schemaTableName, String columnName) {
    String[] columnNames = {columnName};
    applyAlter(schemaTableName, TableChange.deleteColumn(columnNames, true));
  }

  public void setColumnComment(SchemaTableName schemaTableName, String columnName, String comment) {
    String[] columnNames = {columnName};
    applyAlter(schemaTableName, TableChange.updateColumnComment(columnNames, comment));
  }

  public void renameColumn(SchemaTableName schemaTableName, String columnName, String target) {
    if (columnName.equals(target)) {
      return;
    }
    String[] columnNames = {columnName};
    applyAlter(schemaTableName, TableChange.renameColumn(columnNames, target));
  }

  public void setColumnType(SchemaTableName schemaTableName, String columnName, Type type) {
    String[] columnNames = {columnName};
    applyAlter(schemaTableName, TableChange.updateColumnType(columnNames, type));
  }
}
