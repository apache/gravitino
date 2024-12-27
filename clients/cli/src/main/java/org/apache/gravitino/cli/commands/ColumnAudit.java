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

import org.apache.gravitino.Catalog;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchColumnException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTableException;

public class ColumnAudit extends AuditCommand {

  protected final String metalake;
  protected final String catalog;
  protected final String schema;
  protected final String table;
  protected final String column;

  /**
   * Displays the audit information of a column.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param table The name of the table.
   * @param column The name of the column.
   */
  public ColumnAudit(
      String url,
      boolean ignoreVersions,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column) {
    super(url, ignoreVersions);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.table = table;
    this.column = column;
  }

  /** Displays the audit information of a specified column. */
  @Override
  public void handle() {
    Catalog result;

    try (GravitinoClient client = buildClient(metalake)) {
      result = client.loadCatalog(this.catalog);
    } catch (NoSuchMetalakeException err) {
      System.err.println(ErrorMessages.UNKNOWN_METALAKE);
      return;
    } catch (NoSuchCatalogException err) {
      System.err.println(ErrorMessages.UNKNOWN_CATALOG);
      return;
    } catch (NoSuchTableException err) {
      System.err.println(ErrorMessages.UNKNOWN_TABLE);
      return;
    } catch (NoSuchColumnException err) {
      System.err.println(ErrorMessages.UNKNOWN_COLUMN);
      return;
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
      return;
    }

    if (result != null) {
      displayAuditInfo(result.auditInfo());
    }
  }
}
