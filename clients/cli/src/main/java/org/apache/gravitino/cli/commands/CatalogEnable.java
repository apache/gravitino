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
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.MetalakeNotInUseException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

/** Enable catalog. */
public class CatalogEnable extends Command {
  private final String metalake;
  private final String catalog;
  private final boolean enableMetalake;

  /**
   * Enable catalog
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param quiet whether to display output.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param enableMetalake Whether to enable it's metalake
   */
  public CatalogEnable(
      String url,
      boolean ignoreVersions,
      boolean quiet,
      String metalake,
      String catalog,
      boolean enableMetalake) {
    super(url, ignoreVersions, quiet);
    this.metalake = metalake;
    this.catalog = catalog;
    this.enableMetalake = enableMetalake;
  }

  /** Enable catalog. */
  @Override
  public void handle() {
    try {
      if (enableMetalake) {
        GravitinoAdminClient adminClient = buildAdminClient();
        adminClient.enableMetalake(metalake);
      }
      GravitinoClient client = buildClient(metalake);
      client.enableCatalog(catalog);
    } catch (NoSuchMetalakeException noSuchMetalakeException) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchCatalogException noSuchCatalogException) {
      exitWithError(ErrorMessages.UNKNOWN_CATALOG);
    } catch (MetalakeNotInUseException notInUseException) {
      exitWithError(
          metalake + " not in use. please use --recursive option, or enable metalake first");
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    if (quiet) return;
    System.out.println(metalake + "." + catalog + " has been enabled.");
  }
}
