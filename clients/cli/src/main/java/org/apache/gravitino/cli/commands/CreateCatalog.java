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

import java.util.Map;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.cli.Providers;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

public class CreateCatalog extends Command {
  protected final String metalake;
  protected final String catalog;
  protected final String provider;
  protected final String comment;
  protected final Map<String, String> properties;

  /**
   * Create a new catalog.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param provider The provider/type of catalog.
   * @param comment The catalog's comment.
   * @param properties The catalog's properties.
   */
  public CreateCatalog(
      String url,
      boolean ignoreVersions,
      String metalake,
      String catalog,
      String provider,
      String comment,
      Map<String, String> properties) {
    super(url, ignoreVersions);
    this.metalake = metalake;
    this.catalog = catalog;
    this.provider = provider;
    this.comment = comment;
    this.properties = properties;
  }

  /** Create a new catalog. */
  @Override
  public void handle() {
    try {
      GravitinoClient client = buildClient(metalake);
      client.createCatalog(
          catalog,
          Providers.catalogType(provider),
          Providers.internal(provider),
          comment,
          properties);
    } catch (NoSuchMetalakeException err) {
      System.err.println(ErrorMessages.METALAKE_EXISTS);
      return;
    } catch (CatalogAlreadyExistsException err) {
      System.err.println(ErrorMessages.CATALOG_EXISTS);
      return;
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
      return;
    }

    System.out.println(catalog + " catalog created");
  }
}
