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
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.TableCatalog;

/* Common code for all table commands. */
public class TableCommand extends AuditCommand {

  protected final String metalake;
  protected final String catalog;

  /**
   * Common code for all table commands.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   */
  public TableCommand(String url, boolean ignoreVersions, String metalake, String catalog) {
    super(url, ignoreVersions);
    this.metalake = metalake;
    this.catalog = catalog;
  }

  /* Overridden in parent - do nothing  */
  @Override
  public void handle() {}

  /**
   * Returns the table catalog for a given metalake and catalog.
   *
   * @return The TableCatalog or null if an error occurs.
   */
  public TableCatalog tableCatalog() {
    try {
      GravitinoClient client = buildClient(metalake);
      return client.loadMetalake(metalake).loadCatalog(catalog).asTableCatalog();
    } catch (NoSuchMetalakeException err) {
      System.err.println(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchCatalogException err) {
      System.err.println(ErrorMessages.UNKNOWN_CATALOG);
    } catch (NoSuchSchemaException err) {
      System.err.println(ErrorMessages.UNKNOWN_SCHEMA);
    } catch (NoSuchTableException err) {
      System.err.println(ErrorMessages.UNKNOWN_TABLE);
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
    }

    return null;
  }
}
