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

import com.google.common.base.Joiner;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.Column;

/** Displays the details of a table's columns. */
public class ListColumns extends TableCommand {

  protected final String schema;
  protected final String table;

  /**
   * Displays the details of a table's columns.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schenma.
   * @param table The name of the table.
   */
  public ListColumns(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    super(context, metalake, catalog);
    this.schema = schema;
    this.table = table;
  }

  /** Displays the details of a table's columns. */
  @Override
  public void handle() {
    Column[] columns = null;

    try {
      NameIdentifier name = NameIdentifier.of(schema, table);
      columns = tableCatalog().loadTable(name).columns();
    } catch (NoSuchTableException noSuchTableException) {
      exitWithError(
          ErrorMessages.UNKNOWN_TABLE + Joiner.on(".").join(metalake, catalog, schema, table));
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    StringBuilder all = new StringBuilder();
    for (int i = 0; i < columns.length; i++) {
      String name = columns[i].name();
      String dataType = columns[i].dataType().simpleString();
      String comment = columns[i].comment();
      String nullable = columns[i].nullable() ? "true" : "false";
      String autoIncrement = columns[i].autoIncrement() ? "true" : "false";

      if (i == 0) {
        all.append("name,datatype,comment,nullable,auto_increment" + System.lineSeparator());
      }
      // TODO default values
      all.append(
          name
              + ","
              + dataType
              + ","
              + comment
              + ","
              + nullable
              + ","
              + autoIncrement
              + System.lineSeparator());
    }

    printResults(all.toString());
  }
}
