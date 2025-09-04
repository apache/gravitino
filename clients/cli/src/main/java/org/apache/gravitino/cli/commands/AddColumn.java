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

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.DefaultConverter;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.cli.ParseType;
import org.apache.gravitino.cli.PositionConverter;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Type;

/** Represents the command for adding a new column to a table. */
public class AddColumn extends Command {

  /** The name of the metalake. */
  protected final String metalake;
  /** The name of the catalog. */
  protected final String catalog;
  /** The name of the schema. */
  protected final String schema;
  /** The name of the table. */
  protected final String table;
  /** The name of the new column. */
  protected final String column;
  /** The data type of the new column. */
  protected final String datatype;
  /** The comment for the column (optional). */
  protected final String comment;
  /** The position of the column (optional). */
  protected final String position;
  /** True if the column can be null, false if it cannot be (optional). */
  protected final boolean nullable;
  /** True if the column auto increments (optional). */
  protected final boolean autoIncrement;
  /** Default value of the column (optional). */
  protected final String defaultValue;

  /**
   * Adds an optional column to a table.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param table The name of the table.
   * @param column The name of the new column.
   * @param datatype The data type of the new column.
   * @param comment The comment for the column (optional).
   * @param position The position of the column (optional).
   * @param nullable True if the column can be null, false if it cannot be (optional).
   * @param autoIncrement True if the column auto increments (optional).
   * @param defaultValue Default value of the column (optional).
   */
  public AddColumn(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column,
      String datatype,
      String comment,
      String position,
      boolean nullable,
      boolean autoIncrement,
      String defaultValue) {
    super(context);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.table = table;
    this.column = column;
    this.datatype = datatype;
    this.comment = comment;
    this.position = position;
    this.nullable = nullable;
    this.autoIncrement = autoIncrement;
    this.defaultValue = defaultValue;
  }

  /** Adds an optional column to a table. */
  @Override
  public void handle() {
    String[] columns = {column};
    Type convertedDatatype = ParseType.toType(datatype);

    try {
      GravitinoClient client = buildClient(metalake);
      NameIdentifier name = NameIdentifier.of(schema, table);
      TableChange change =
          TableChange.addColumn(
              columns,
              convertedDatatype,
              comment,
              PositionConverter.convert(position),
              nullable,
              autoIncrement,
              DefaultConverter.convert(defaultValue, datatype));

      client.loadCatalog(catalog).asTableCatalog().alterTable(name, change);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchCatalogException err) {
      exitWithError(ErrorMessages.UNKNOWN_CATALOG);
    } catch (NoSuchSchemaException err) {
      exitWithError(ErrorMessages.UNKNOWN_SCHEMA);
    } catch (NoSuchTableException err) {
      exitWithError(ErrorMessages.UNKNOWN_TABLE);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    printInformation(column + " added to table " + table + ".");
  }
}
