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

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.cli.Providers;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

public class CreateCatalog extends Command {
  protected String metalake;
  protected String catalog;
  protected String provider;
  protected String comment;
  Map<String, String> properties;

  /**
   * Create a new catalog.
   *
   * @param url The URL of the Gravitino server.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param provider The provider/type of catalog.
   * @param comment The catalog's comment.
   */
  public CreateCatalog(
      String url, String metalake, String catalog, String provider, String comment) {
    super(url);
    this.metalake = metalake;
    this.catalog = catalog;
    this.provider = provider;
    this.comment = comment;
    properties = new HashMap<>();
  }

  /** Create a new catalog. */
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

    System.out.println(catalog + " created");
  }
}
