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
package org.apache.gravitino.trino.connector.catalog;

import com.google.common.base.Strings;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.metadata.GravitinoColumn;
import org.apache.gravitino.trino.connector.metadata.GravitinoSchema;
import org.apache.gravitino.trino.connector.metadata.GravitinoTable;

/** This class implements Apache Gravitino metadata operators. */
public class CatalogConnectorMetadata {

  private static final String CATALOG_DOES_NOT_EXIST_MSG = "Catalog does not exist";
  private static final String SCHEMA_DOES_NOT_EXIST_MSG = "Schema does not exist";

  private final String catalogName;
  private final SupportsSchemas schemaCatalog;
  private final TableCatalog tableCatalog;

  public CatalogConnectorMetadata(GravitinoMetalake metalake, NameIdentifier catalogIdentifier) {
    try {
      this.catalogName = catalogIdentifier.name();
      Catalog catalog = metalake.loadCatalog(catalogName);
      // Make sure the catalog support schema operations.
      this.schemaCatalog = catalog.asSchemas();
      this.tableCatalog = catalog.asTableCatalog();
    } catch (NoSuchCatalogException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_CATALOG_NOT_EXISTS, CATALOG_DOES_NOT_EXIST_MSG, e);
    } catch (UnsupportedOperationException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION,
          "Catalog does not support schema or table operations." + e.getMessage(),
          e);
    }
  }

  public List<String> listSchemaNames() {
    try {
      return Arrays.asList(schemaCatalog.listSchemas());
    } catch (NoSuchCatalogException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_CATALOG_NOT_EXISTS, CATALOG_DOES_NOT_EXIST_MSG, e);
    }
  }

  public GravitinoSchema getSchema(String schemaName) {
    try {
      Schema schema = schemaCatalog.loadSchema(schemaName);
      return new GravitinoSchema(schema);
    } catch (NoSuchSchemaException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_SCHEMA_NOT_EXISTS, SCHEMA_DOES_NOT_EXIST_MSG, e);
    }
  }

  public GravitinoTable getTable(String schemaName, String tableName) {
    try {
      Table table = tableCatalog.loadTable(NameIdentifier.of(schemaName, tableName));
      return new GravitinoTable(schemaName, tableName, table);
    } catch (NoSuchTableException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_TABLE_NOT_EXISTS, "Table does not exist", e);
    }
  }

  public List<String> listTables(String schemaName) {
    try {
      NameIdentifier[] tables = tableCatalog.listTables(Namespace.of(schemaName));
      return Arrays.stream(tables).map(NameIdentifier::name).toList();
    } catch (NoSuchSchemaException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_SCHEMA_NOT_EXISTS, SCHEMA_DOES_NOT_EXIST_MSG, e);
    }
  }

  public boolean tableExists(String schemaName, String tableName) {
    return tableCatalog.tableExists(NameIdentifier.of(schemaName, tableName));
  }

  public void createTable(GravitinoTable table, boolean ignoreExisting) {
    NameIdentifier identifier = NameIdentifier.of(table.getSchemaName(), table.getName());
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
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_SCHEMA_NOT_EXISTS, SCHEMA_DOES_NOT_EXIST_MSG, e);
    } catch (TableAlreadyExistsException e) {
      if (!ignoreExisting) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_TABLE_ALREADY_EXISTS, "Table already exists", e);
      }
    }
  }

  public void createSchema(GravitinoSchema schema) {
    try {
      schemaCatalog.createSchema(schema.getName(), schema.getComment(), schema.getProperties());
    } catch (NoSuchSchemaException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_CATALOG_NOT_EXISTS, CATALOG_DOES_NOT_EXIST_MSG, e);
    } catch (TableAlreadyExistsException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_SCHEMA_ALREADY_EXISTS, "Schema already exists", e);
    }
  }

  public void dropSchema(String schemaName, boolean cascade) {
    try {
      boolean success = schemaCatalog.dropSchema(schemaName, cascade);

      if (!success) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_SCHEMA_NOT_EXISTS, "Drop schema failed");
      }

    } catch (NonEmptySchemaException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_SCHEMA_NOT_EMPTY, "Schema does not empty", e);
    }
  }

  public void dropTable(SchemaTableName tableName) {
    boolean dropped =
        tableCatalog.dropTable(
            NameIdentifier.of(tableName.getSchemaName(), tableName.getTableName()));
    if (!dropped) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_OPERATION_FAILED, "Failed to drop table " + tableName);
    }
  }

  public void renameSchema(String source, String target) {
    throw new NotImplementedException();
  }

  private void applyAlter(SchemaTableName tableName, TableChange... change) {
    try {
      tableCatalog.alterTable(
          NameIdentifier.of(tableName.getSchemaName(), tableName.getTableName()), change);
    } catch (NoSuchTableException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_TABLE_NOT_EXISTS, "Table does not exist");
    } catch (IllegalArgumentException e) {
      // TODO yuhui need improve get the error message. From IllegalArgumentException.
      // At present, the IllegalArgumentException cannot get the error information clearly from the
      // Gravitino server.
      String message =
          e.getMessage().lines().toList().get(0) + e.getMessage().lines().toList().get(1);
      throw new TrinoException(GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT, message, e);
    }
  }

  public void renameTable(SchemaTableName oldTableName, SchemaTableName newTableName) {
    if (!oldTableName.getSchemaName().equals(newTableName.getSchemaName())) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION, "Cannot rename table across schemas");
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
