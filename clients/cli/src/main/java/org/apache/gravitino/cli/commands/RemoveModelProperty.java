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
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.model.ModelChange;

/** Removes a property of a model. */
public class RemoveModelProperty extends Command {
  /** The name of the metalake. */
  protected final String metalake;
  /** The name of the catalog. */
  protected final String catalog;
  /** The name of the schema. */
  protected final String schema;
  /** The name of the model. */
  protected final String model;
  /** The name of the property to remove. */
  protected final String property;

  /**
   * Constructs a new {@link RemoveModelProperty} instance.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @param model The name of the model
   * @param property The name of the property to remove
   */
  public RemoveModelProperty(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String model,
      String property) {
    super(context);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.model = model;
    this.property = property;
  }

  /** Removes a property of a model. */
  @Override
  public void handle() {
    try {
      NameIdentifier name = NameIdentifier.of(schema, model);
      GravitinoClient client = buildClient(metalake);
      ModelChange change = ModelChange.removeProperty(property);
      client.loadCatalog(catalog).asModelCatalog().alterModel(name, change);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchCatalogException err) {
      exitWithError(ErrorMessages.UNKNOWN_CATALOG);
    } catch (NoSuchSchemaException err) {
      exitWithError(ErrorMessages.UNKNOWN_SCHEMA);
    } catch (NoSuchModelException err) {
      exitWithError(ErrorMessages.UNKNOWN_MODEL);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    printInformation(property + " property removed.");
  }

  /** {@inheritDoc} */
  @Override
  public Command validate() {
    validateProperty(property);
    return super.validate();
  }
}
