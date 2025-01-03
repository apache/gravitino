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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;

/** Creates a new model. */
public class CreateModel extends Command {

  protected final String metalake;
  protected final String catalog;
  protected final String schema;
  protected final String model;
  protected final String comment;
  protected final Map<String, String> properties;

  /**
   * Creates a new model.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param model The name of the model.
   * @param comment The comment for the model.
   * @param properties The model's properties.
   */
  public CreateModel(
      String url,
      boolean ignoreVersions,
      String metalake,
      String catalog,
      String schema,
      String model,
      String comment,
      Map<String, String> properties) {
    super(url, ignoreVersions);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.model = model;
    this.comment = comment;
    this.properties = properties;
  }

  /** Creates a new model. */
  public void handle() {
    try (GravitinoClient client = buildClient(metalake)) {
      NameIdentifier name = NameIdentifier.of(schema, model);
      client.loadCatalog(catalog).asModelCatalog().registerModel(name, comment, properties);
    } catch (NoSuchMetalakeException noSuchMetalakeException) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchCatalogException noSuchCatalogException) {
      exitWithError(ErrorMessages.UNKNOWN_CATALOG);
    } catch (NoSuchSchemaException noSuchSchemaException) {
      exitWithError(ErrorMessages.UNKNOWN_SCHEMA);
    } catch (ModelAlreadyExistsException modelAlreadyExistsException) {
      exitWithError(ErrorMessages.MODEL_EXISTS);
    } catch (Exception err) {
      exitWithError(err.getMessage());
    }

    System.out.println(model + " created");
  }
}
