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

import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;

public class CreateTable extends Command {
  protected final String metalake;
  protected final String catalog;
  protected final String schema;
  protected final String table;
  protected final String columns;
  protected final String comment;

  /**
   * Create a new table.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param table The name of the table.
   * @param columns The file name containing the CSV column info.
   * @param comment The schema's comment.
   */
  public CreateTable(
      String url,
      boolean ignoreVersions,
      String metalake,
      String catalog,
      String schema,
      String table,
      String columns,
      String comment) {
    super(url, ignoreVersions);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.table = table;
    this.columns = columns;
    this.comment = comment;
  }

  /** Create a new schema. */
  @Override
  public void handle() {
    try {
      NameIdentifier tableName = NameIdentifier.of(schema, table);
      GravitinoClient client = buildClient(metalake);
      Column[] columns;
      ReadTableCSV readTableCSV = new ReadTableCSV();
      Map<String, List<String>> tableData = readTableCSV.parse(tempFile.toString());
      List<String> names = data.tableData.get("Name");  
      List<String> datatypes = data.tableData.get("Datatype");
      List<String> comments = tableData.get("Comment");
      List<String> nullables = tableData.get("Nullable");
      List<String> autos = tableData.get("AutoIncrement");
      List<String> defaultTypes = tableData.get("DefaultValue");
      List<String> defaulValues = tableData.get("DefaultType");

      for (i = 0; i < name.size(); i++) {
        String columnName = names.get(i);
        String datatype = datatypes.get(i);
        String comment = comments.get(i);
        String nullable = nullables.get(i);
        String auto = autos.get(i);
        String defaultType = defaultTypes.get(i);
        String defaulValue = defaulValues.get(i);
        Column column = new Column(columnName, datatype, comment, nullable, auto, defaultType, defaulValue);
        columns.add(column);
      }

      client.loadCatalog(catalog).asSchemas().createTable(tableName, columns, comment, null);
    } catch (NoSuchMetalakeException err) {
      System.err.println(ErrorMessages.UNKNOWN_METALAKE);
      return;
    } catch (NoSuchCatalogException err) {
      System.err.println(ErrorMessages.UNKNOWN_CATALOG);
      return;
    } catch (NoSuchSchemaException err) {
      System.err.println(ErrorMessages.UNKNOWN_SCHEMA);
      return;
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
      return;
    }

    System.out.println(schema + " created");
  }
}
