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

import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

/** Disable catalog. */
public class CatalogDisable extends Command {

  private final String metalake;
  private final String catalog;

  /**
   * Disable catalog
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   */
  public CatalogDisable(CommandContext context, String metalake, String catalog) {
    super(context);
    this.metalake = metalake;
    this.catalog = catalog;
  }

  /** Disable catalog. */
  @Override
  public void handle() {
    try {
      GravitinoClient client = buildClient(metalake);
      client.disableCatalog(catalog);
    } catch (NoSuchMetalakeException noSuchMetalakeException) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchCatalogException noSuchCatalogException) {
      exitWithError(ErrorMessages.UNKNOWN_CATALOG);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    printInformation(metalake + "." + catalog + " has been disabled.");
  }
}
