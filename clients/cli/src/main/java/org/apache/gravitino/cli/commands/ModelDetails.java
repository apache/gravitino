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

import java.util.Arrays;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelCatalog;

/** Displays the details of a model. */
public class ModelDetails extends Command {
  protected final String metalake;
  protected final String catalog;
  protected final String schema;
  protected final String model;

  /**
   * Displays the details of a model.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of schema.
   * @param model The name of model.
   */
  public ModelDetails(
      CommandContext context, String metalake, String catalog, String schema, String model) {
    super(context);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.model = model;
  }

  /** Displays the details of a model. */
  @Override
  public void handle() {
    NameIdentifier name = NameIdentifier.of(schema, model);
    Model gModel = null;
    int[] versions = new int[0];

    try {
      GravitinoClient client = buildClient(metalake);
      ModelCatalog modelCatalog = client.loadCatalog(catalog).asModelCatalog();
      gModel = modelCatalog.getModel(name);
      versions = modelCatalog.listModelVersions(name);
    } catch (NoSuchMetalakeException noSuchMetalakeException) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchCatalogException noSuchCatalogException) {
      exitWithError(ErrorMessages.UNKNOWN_CATALOG);
    } catch (NoSuchSchemaException noSuchSchemaException) {
      exitWithError(ErrorMessages.UNKNOWN_SCHEMA);
    } catch (NoSuchModelException noSuchModelException) {
      exitWithError(ErrorMessages.UNKNOWN_MODEL);
    } catch (Exception err) {
      exitWithError(err.getMessage());
    }
    String basicInfo =
        String.format("Model name %s, latest version: %s%n", gModel.name(), gModel.latestVersion());
    String versionInfo = Arrays.toString(versions);
    printResults(basicInfo + "versions: " + versionInfo);
  }
}
