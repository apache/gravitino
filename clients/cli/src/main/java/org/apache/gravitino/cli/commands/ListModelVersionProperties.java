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
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.model.ModelCatalog;
import org.apache.gravitino.model.ModelVersion;

/** List the properties of a model version. */
public class ListModelVersionProperties extends ListProperties {

  /** The name of the metalake. */
  protected final String metalake;
  /** The name of the catalog. */
  protected final String catalog;
  /** The name of the schema. */
  protected final String schema;
  /** The name of the model. */
  protected final String model;
  /** The version number of the model version. */
  protected final Integer version;
  /** The alias of the model version. */
  protected final String alias;

  /**
   * List the properties of a model version.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param model The name of the model.
   * @param version The version number of the model version.
   * @param alias The alias of the model version.
   */
  public ListModelVersionProperties(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String model,
      Integer version,
      String alias) {
    super(context);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.model = model;
    this.version = version;
    this.alias = alias;
  }

  /** List the properties of a model version. */
  @Override
  public void handle() {
    if (version == null && alias == null) {
      exitWithError("Either --version or --alias must be specified.");
      return;
    }

    ModelVersion gModelVersion = null;
    try {
      NameIdentifier name = NameIdentifier.of(schema, model);
      GravitinoClient client = buildClient(metalake);
      ModelCatalog modelCatalog = client.loadCatalog(catalog).asModelCatalog();
      if (version != null) {
        gModelVersion = modelCatalog.getModelVersion(name, version);
      } else {
        gModelVersion = modelCatalog.getModelVersion(name, alias);
      }
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchCatalogException err) {
      exitWithError(ErrorMessages.UNKNOWN_CATALOG);
    } catch (NoSuchSchemaException err) {
      exitWithError(ErrorMessages.UNKNOWN_SCHEMA);
    } catch (NoSuchModelException err) {
      exitWithError(ErrorMessages.UNKNOWN_MODEL);
    } catch (NoSuchModelVersionException err) {
      exitWithError(ErrorMessages.UNKNOWN_MODEL_VERSION);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    Map<String, String> properties = gModelVersion.properties();
    printProperties(properties);
  }
}
