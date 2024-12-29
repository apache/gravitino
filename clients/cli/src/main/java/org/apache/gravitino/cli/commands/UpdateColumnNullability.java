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
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchColumnException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.TableChange;

/** Update the nullability of a column. */
public class UpdateColumnNullability extends Command {

  protected final String metalake;
  protected final String catalog;
  protected final String schema;
  protected final String table;
  protected final String column;
  protected final boolean nullability;

  /**
   * Update the nullability of a column.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param table The name of the table.
   * @param column The name of the column.
   * @param nullability True if the column can be null, false if it must have non-null values.
   */
  public UpdateColumnNullability(
      String url,
      boolean ignoreVersions,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column,
      boolean nullability) {
    super(url, ignoreVersions);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.table = table;
    this.column = column;
    this.nullability = nullability;
  }

  /** Update the nullability of a column. */
  @Override
  public void handle() {
    String[] columns = {column};

    try {
      NameIdentifier columnName = NameIdentifier.of(schema, table);
      GravitinoClient client = buildClient(metalake);
      TableChange change = TableChange.updateColumnNullability(columns, nullability);

      client.loadCatalog(catalog).asTableCatalog().alterTable(columnName, change);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchCatalogException err) {
      exitWithError(ErrorMessages.UNKNOWN_CATALOG);
    } catch (NoSuchSchemaException err) {
      exitWithError(ErrorMessages.UNKNOWN_SCHEMA);
    } catch (NoSuchTableException err) {
      exitWithError(ErrorMessages.UNKNOWN_TABLE);
    } catch (NoSuchColumnException err) {
      exitWithError(ErrorMessages.UNKNOWN_COLUMN);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    System.out.println(column + " nullability changed to " + nullability + ".");
  }
}
