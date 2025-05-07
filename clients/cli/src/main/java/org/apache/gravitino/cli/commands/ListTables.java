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

/** List the names of all tables in a schema. */
public class ListTables extends TableCommand {

  protected final String schema;

  /**
   * List the names of all tables in a schema.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   */
  public ListTables(CommandContext context, String metalake, String catalog, String schema) {
    super(context, metalake, catalog);
    this.schema = schema;
  }

  /** List the names of all tables in a schema. */
  @Override
  public void handle() {

      Namespace name = Namespace.of(schema);
      NameIdentifier[] tables = tableCatalog().listTables(name);

      if (tables == null || tables.length == 0) {
        printInformation("No tables exist.");
        return;
      }
      
      try {
      Table[] gTables = new Table[tables.length];
      for (int i = 0; i < tables.length; i++) {
        String tableName = tables[i].name();
        gTables[i] = createTableStub(tableName);
      }

      printResults(gTables);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }
  }

  /**
   * Creates a stub Table instance with only the table name.
   *
   * @param tableName The name of the table.
   * @return A minimal Table instance.
   */
  private Table createTableStub(String tableName) {
    return new Table() {
      @Override
      public String name() {
        return tableName;
      }

      @Override
      public Column[] columns() {
        return new Column[0]; // Empty columns since only table names are needed
      }

      @Override
      public Audit auditInfo() {
        return null; // No audit info needed for listing tables
      }
    };
  }
}
