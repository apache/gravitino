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

import java.util.Arrays;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.rel.indexes.Index;

/** Displays the index of a table. */
public class ListIndexes extends TableCommand {

  protected final String schema;
  protected final String table;

  /**
   * Displays the index of a table.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param table The name of the table.
   */
  public ListIndexes(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    super(context, metalake, catalog);
    this.schema = schema;
    this.table = table;
  }

  /** Displays the details of a table's index. */
  @Override
  public void handle() {
    Index[] indexes = {};

    try {
      NameIdentifier name = NameIdentifier.of(schema, table);
      indexes = tableCatalog().loadTable(name).index();
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    StringBuilder all = new StringBuilder();
    for (Index index : indexes) {
      // Flatten nested field names into dot-separated strings (e.g., "a.b.c")
      Arrays.stream(index.fieldNames())
          // Convert nested fields to a single string
          .map(nestedFieldName -> String.join(".", nestedFieldName))
          .forEach(
              fieldName ->
                  all.append(fieldName)
                      .append(",")
                      .append(index.name())
                      .append(System.lineSeparator()));
    }

    printResults(all.toString());
  }
}
