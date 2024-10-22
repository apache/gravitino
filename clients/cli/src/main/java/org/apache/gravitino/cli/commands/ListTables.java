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
import org.apache.gravitino.Namespace;

/** List the names of all tables in a schema. */
public class ListTables extends TableCommand {

  protected final String schema;

  /**
   * List the names of all tables in a schema.
   *
   * @param url The URL of the Gravitino server.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schenma.
   */
  public ListTables(String url, String metalake, String catalog, String schema) {
    super(url, metalake, catalog);
    this.schema = schema;
  }

  /** List the names of all tables in a schema. */
  public void handle() {
    NameIdentifier[] tables = null;
    Namespace name = Namespace.of(schema);
    try {
      tables = tableCatalog().listTables(name);
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
      return;
    }

    StringBuilder all = new StringBuilder();
    for (int i = 0; i < tables.length; i++) {
      if (i > 0) {
        all.append(",");
      }
      all.append(tables[i].name());
    }

    System.out.println(all.toString());
  }
}
