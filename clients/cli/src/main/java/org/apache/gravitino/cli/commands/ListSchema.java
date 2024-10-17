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

/** List all schema names in a schema. */
public class ListSchema extends Command {

  protected String metalake;
  protected String catalog;

  /**
   * Lists all schemas in a catalog.
   *
   * @param url The URL of the Gravitino server.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   */
  public ListSchema(String url, String metalake, String catalog) {
    super(url);
    this.metalake = metalake;
    this.catalog = catalog;
  }

  /** List all schema names in a schema. */
  public void handle() {
    String[] schemas = new String[0];
    try {
      GravitinoClient client = buildClient(metalake);
      schemas = client.loadCatalog(catalog).asSchemas().listSchemas();
    } catch (NoSuchMetalakeException err) {
      System.err.println(ErrorMessages.UNKNOWN_METALAKE);
      return;
    } catch (NoSuchCatalogException err) {
      System.err.println(ErrorMessages.UNKNOWN_CATALOG);
      return;
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
      return;
    }

    StringBuilder all = new StringBuilder();
    for (int i = 0; i < schemas.length; i++) {
      if (i > 0) {
        all.append(",");
      }
      all.append(schemas[i]);
    }

    System.out.println(all.toString());
  }
}
