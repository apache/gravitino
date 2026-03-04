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

import org.apache.gravitino.Audit;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;

/** List the names of all tables in a schema. */
public class ListTables extends TableCommand {

  /** The name of the schema. */
  protected final String schema;

  /**
   * List the names of all tables in a schema.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schenma.
   */
  public ListTables(CommandContext context, String metalake, String catalog, String schema) {
    super(context, metalake, catalog);
    this.schema = schema;
  }

  /** List the names of all tables in a schema. */
  @Override
  public void handle() {
    NameIdentifier[] tables = null;
    Namespace name = Namespace.of(schema);

    TableCatalog catalog = tableCatalog();
    if (catalog == null) {
      // The error message is already printed in the tableCatalog() method.
      // We just need to exit here.
      return;
    }

    try {
      tables = catalog.listTables(name);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    if (tables == null || tables.length == 0) {
      printInformation("No tables exist.");
      return;
    }

    Table[] gTables = new Table[tables.length];
    for (int i = 0; i < tables.length; i++) {
      String tableName = tables[i].name();
      gTables[i] =
          new Table() {

            @Override
            public String name() {
              return tableName;
            }

            @Override
            public Column[] columns() {
              return new Column[0];
            }

            @Override
            public Audit auditInfo() {
              return null;
            }
          };
    }

    printResults(gTables);
  }
}
