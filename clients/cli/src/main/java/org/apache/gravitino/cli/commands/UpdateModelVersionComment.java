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

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.model.ModelVersionChange;

/** Update the comment of a model version. */
public class UpdateModelVersionComment extends Command {
  /** The name of the metalake. */
  protected final String metalake;
  /** The name of the catalog. */
  protected final String catalog;
  /** The name of the schema. */
  protected final String schema;
  /** The name of the model. */
  protected final String model;
  /** The version of the model. */
  protected final Integer version;

  private final String alias;
  private final String comment;

  /**
   * Constructs a new {@link UpdateModelVersionComment} instance.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param model The name of the model.
   * @param version The version of the model.
   * @param alias The alias of the model version.
   * @param comment The new comment for the model version.
   */
  public UpdateModelVersionComment(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String model,
      Integer version,
      String alias,
      String comment) {
    super(context);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.model = model;
    this.version = version;
    this.alias = alias;
    this.comment = comment;
  }

  /** Update the comment of a model version. */
  @Override
  public void handle() {
    try {
      NameIdentifier modelIdent = NameIdentifier.of(schema, model);
      GravitinoClient client = buildClient(metalake);
      ModelVersionChange change = ModelVersionChange.updateComment(comment);
      if (alias != null) {
        client.loadCatalog(catalog).asModelCatalog().alterModelVersion(modelIdent, alias, change);
      } else {
        client.loadCatalog(catalog).asModelCatalog().alterModelVersion(modelIdent, version, change);
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

    if (alias != null) {
      printInformation(model + " alias " + alias + " comment changed.");
    } else {
      printInformation(model + " version " + version + " comment changed.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public Command validate() {
    if (alias != null && version != null) {
      exitWithError("Cannot specify both alias and version");
    }

    return this;
  }
}
