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
import org.apache.gravitino.cli.AreYouSure;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;

public class DeleteTable extends Command {

  protected final String metalake;
  protected final String catalog;
  protected final String schema;
  protected final String table;
  protected final boolean force;

  /**
   * Delete a table.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param table The name of the table.
   */
  public DeleteTable(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    super(context);
    this.force = context.force();
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.table = table;
  }

  /** Delete a table. */
  @Override
  public void handle() {
    boolean deleted = false;

    if (!AreYouSure.really(force)) {
      return;
    }

    try {
      GravitinoClient client = buildClient(metalake);
      NameIdentifier name = NameIdentifier.of(schema, table);
      deleted = client.loadCatalog(catalog).asTableCatalog().dropTable(name);
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

    if (deleted) {
      printInformation(table + " deleted.");
    } else {
      printInformation(table + " not deleted.");
    }
  }
}
