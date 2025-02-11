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

/** Link a new model version to the registered model. */
import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.model.ModelCatalog;

public class LinkModel extends Command {
  protected final String metalake;
  protected final String catalog;
  protected final String schema;
  protected final String model;
  protected final String uri;
  protected final String[] alias;
  protected final String comment;
  protected final Map<String, String> properties;

  /**
   * Link a new model version to the registered model.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of schema.
   * @param model The name of model.
   * @param uri The URI of the model version artifact.
   * @param alias The aliases of the model version.
   * @param comment The comment of the model version.
   * @param properties The properties of the model version.
   */
  public LinkModel(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String model,
      String uri,
      String[] alias,
      String comment,
      Map<String, String> properties) {
    super(context);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.model = model;
    this.uri = uri;
    this.alias = alias;
    this.comment = comment;
    this.properties = properties;
  }

  /** Link a new model version to the registered model. */
  @Override
  public void handle() {
    NameIdentifier name = NameIdentifier.of(schema, model);

    try {
      GravitinoClient client = buildClient(metalake);
      ModelCatalog modelCatalog = client.loadCatalog(catalog).asModelCatalog();
      modelCatalog.linkModelVersion(name, uri, alias, comment, properties);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchCatalogException err) {
      exitWithError(ErrorMessages.UNKNOWN_CATALOG);
    } catch (NoSuchSchemaException err) {
      exitWithError(ErrorMessages.UNKNOWN_SCHEMA);
    } catch (NoSuchModelException err) {
      exitWithError(ErrorMessages.UNKNOWN_MODEL);
    } catch (ModelVersionAliasesAlreadyExistException err) {
      exitWithError(Arrays.toString(alias) + " already exist.");
    } catch (Exception err) {
      exitWithError(err.getMessage());
    }

    printResults(
        "Linked model " + model + " to " + uri + " with aliases " + Arrays.toString(alias));
  }

  @Override
  public Command validate() {
    if (uri == null) exitWithError(ErrorMessages.MISSING_URI);
    return super.validate();
  }
}
