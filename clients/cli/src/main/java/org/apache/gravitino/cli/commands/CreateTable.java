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

package org.apache.gravitino.cli.commands;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.cli.ReadTableCSV;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.Column;

public class CreateTable extends Command {
  protected final String metalake;
  protected final String catalog;
  protected final String schema;
  protected final String table;
  protected final String columnFile;
  protected final String comment;

  /**
   * Create a new table.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param table The name of the table.
   * @param columnFile The file name containing the CSV column info.
   * @param comment The table's comment.
   */
  public CreateTable(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String table,
      String columnFile,
      String comment) {
    super(context);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.table = table;
    this.columnFile = columnFile;
    this.comment = comment;
  }

  /** Create a new table. */
  @Override
  public void handle() {
    NameIdentifier tableName = null;
    GravitinoClient client = null;
    ReadTableCSV readTableCSV = new ReadTableCSV();
    Map<String, List<String>> tableData;
    Column[] columns = {};

    try {
      tableName = NameIdentifier.of(schema, table);
      client = buildClient(metalake);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (Exception exp) {
      exitWithError("Error initializing client or table name: " + exp.getMessage());
    }

    try {
      tableData = readTableCSV.parse(columnFile);
      columns = readTableCSV.columns(tableData);
    } catch (Exception exp) {
      exitWithError("Error reading or parsing column file: " + exp.getMessage());
    }

    try {
      client.loadCatalog(catalog).asTableCatalog().createTable(tableName, columns, comment, null);
    } catch (NoSuchCatalogException err) {
      exitWithError(ErrorMessages.UNKNOWN_CATALOG);
    } catch (NoSuchSchemaException err) {
      exitWithError(ErrorMessages.UNKNOWN_SCHEMA);
    } catch (TableAlreadyExistsException err) {
      exitWithError(ErrorMessages.TABLE_EXISTS);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    printInformation(table + " created");
  }

  @Override
  public Command validate() {
    if (columnFile == null) exitWithError(ErrorMessages.MISSING_COLUMN_FILE);
    return super.validate();
  }
}
