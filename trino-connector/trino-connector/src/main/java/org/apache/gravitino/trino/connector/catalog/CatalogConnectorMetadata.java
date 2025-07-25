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

  /**
   * Constructs a new CatalogConnectorMetadata.
   *
   * @param metalake the Gravitino metalake
   * @param catalogIdentifier the name of the catalog
   */
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

  /**
   * Lists the names of all schemas in the catalog.
   *
   * @return a list of schema names
   */
  public List<String> listSchemaNames() {
    try {
      return Arrays.asList(schemaCatalog.listSchemas());
    } catch (NoSuchCatalogException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_CATALOG_NOT_EXISTS, CATALOG_DOES_NOT_EXIST_MSG, e);
    }
  }

  /**
   * Retrieves the Gravitino schema for the specified name.
   *
   * @param schemaName the name of the schema
   * @return the Gravitino schema
   * @throws TrinoException if the schema is not found
   */
  public GravitinoSchema getSchema(String schemaName) {
    try {
      Schema schema = schemaCatalog.loadSchema(schemaName);
      return new GravitinoSchema(schema);
    } catch (NoSuchSchemaException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_SCHEMA_NOT_EXISTS, SCHEMA_DOES_NOT_EXIST_MSG, e);
    }
  }

  /**
   * Retrieves the Gravitino table for the specified name.
   *
   * @param schemaName the name of the schema
   * @param tableName the name of the table
   * @return the Gravitino table
   * @throws TrinoException if the table is not found
   */
  public GravitinoTable getTable(String schemaName, String tableName) {
    try {
      Table table = tableCatalog.loadTable(NameIdentifier.of(schemaName, tableName));
      return new GravitinoTable(schemaName, tableName, table);
    } catch (NoSuchTableException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_TABLE_NOT_EXISTS, "Table does not exist", e);
    }
  }

  /**
   * Lists the names of all tables in the specified schema.
   *
   * @param schemaName the name of the schema
   * @return a list of table names
   */
  public List<String> listTables(String schemaName) {
    try {
      NameIdentifier[] tables = tableCatalog.listTables(Namespace.of(schemaName));
      return Arrays.stream(tables).map(NameIdentifier::name).toList();
    } catch (NoSuchSchemaException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_SCHEMA_NOT_EXISTS, SCHEMA_DOES_NOT_EXIST_MSG, e);
    }
  }

  /**
   * Checks if a table exists in the specified schema.
   *
   * @param schemaName the name of the schema
   * @param tableName the name of the table
   * @return true if the table exists, false otherwise
   */
  public boolean tableExists(String schemaName, String tableName) {
    return tableCatalog.tableExists(NameIdentifier.of(schemaName, tableName));
  }

  /**
   * Creates a new table in the catalog.
   *
   * @param table the Gravitino table
   * @param ignoreExisting whether to ignore if the table already exists
   */
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
          table.getSortOrders(),
          table.getIndexes());
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

  /**
   * Creates a new schema in the catalog.
   *
   * @param schema the Gravitino schema
   */
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

  /**
   * Drops a schema from the catalog.
   *
   * @param schemaName the name of the schema
   * @param cascade whether to cascade drop the schema
   */
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

  /**
   * Drops a table from the catalog.
   *
   * @param tableName the name of the table
   */
  public void dropTable(SchemaTableName tableName) {
    boolean dropped =
        tableCatalog.dropTable(
            NameIdentifier.of(tableName.getSchemaName(), tableName.getTableName()));
    if (!dropped) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_OPERATION_FAILED, "Failed to drop table " + tableName);
    }
  }

  /**
   * Renames a schema in the catalog.
   *
   * @param source the name of the source schema
   * @param target the name of the target schema
   */
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
      String message = GravitinoErrorCode.toSimpleErrorMessage(e);
      throw new TrinoException(GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT, message, e);
    }
  }

  /**
   * Renames a table in the catalog.
   *
   * @param oldTableName the old name of the table
   * @param newTableName the new name of the table
   */
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

  /**
   * Sets the comment of the specified table.
   *
   * @param schemaTableName the name of the schema and table
   * @param comment the comment to set
   */
  public void setTableComment(SchemaTableName schemaTableName, String comment) {
    applyAlter(schemaTableName, TableChange.updateComment(comment));
  }

  /**
   * Sets the properties of the specified table.
   *
   * @param schemaTableName the name of the schema and table
   * @param properties the properties to set
   */
  public void setTableProperties(SchemaTableName schemaTableName, Map<String, String> properties) {
    Map<String, String> oldProperties =
        getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName()).getProperties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (!entry.getValue().equals(oldProperties.get(entry.getKey()))) {
        applyAlter(schemaTableName, TableChange.setProperty(entry.getKey(), entry.getValue()));
      }
    }
  }

  /**
   * Adds a new column to the specified table.
   *
   * @param schemaTableName the name of the schema and table
   * @param column the Gravitino column to add
   */
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

  /**
   * Drops a column from the specified table.
   *
   * @param schemaTableName the name of the schema and table
   * @param columnName the name of the column to drop
   */
  public void dropColumn(SchemaTableName schemaTableName, String columnName) {
    String[] columnNames = {columnName};
    applyAlter(schemaTableName, TableChange.deleteColumn(columnNames, true));
  }

  /**
   * Sets the comment of the specified column.
   *
   * @param schemaTableName the name of the schema and table
   * @param columnName the name of the column to set the comment
   * @param comment the comment to set
   */
  public void setColumnComment(SchemaTableName schemaTableName, String columnName, String comment) {
    String[] columnNames = {columnName};
    applyAlter(schemaTableName, TableChange.updateColumnComment(columnNames, comment));
  }

  /**
   * Renames a column in the specified table.
   *
   * @param schemaTableName the name of the schema and table
   * @param columnName the name of the column to rename
   * @param target the new name of the column
   */
  public void renameColumn(SchemaTableName schemaTableName, String columnName, String target) {
    if (columnName.equals(target)) {
      return;
    }
    String[] columnNames = {columnName};
    applyAlter(schemaTableName, TableChange.renameColumn(columnNames, target));
  }

  /**
   * Sets the type of the specified column.
   *
   * @param schemaTableName the name of the schema and table
   * @param columnName the name of the column to set the type
   * @param type the type to set
   */
  public void setColumnType(SchemaTableName schemaTableName, String columnName, Type type) {
    String[] columnNames = {columnName};
    applyAlter(schemaTableName, TableChange.updateColumnType(columnNames, type));
  }
}
