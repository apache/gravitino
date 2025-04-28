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

/** Set or update a property of a model version. */
public class SetModelVersionProperty extends Command {
  private final String metalake;
  private final String catalog;
  private final String schema;
  private final String model;
  private final Integer version;
  private final String alias;
  private final String property;
  private final String value;

  /**
   * Constructs a new {@link SetModelVersionProperty} instance.
   *
   * @param context The command context.
   * @param metalake The metalake name.
   * @param catalog The catalog name.
   * @param schema The schema name.
   * @param model The model name.
   * @param version The model version, if specified, otherwise null.
   * @param alias The model alias, if specified, otherwise null.
   * @param property The property name.
   * @param value The property value.
   */
  public SetModelVersionProperty(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String model,
      Integer version,
      String alias,
      String property,
      String value) {
    super(context);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.model = model;
    this.version = version;
    this.alias = alias;
    this.property = property;
    this.value = value;
  }

  /** Set or update a property of a model version. */
  @Override
  public void handle() {
    try {
      NameIdentifier name = NameIdentifier.of(schema, model);
      GravitinoClient client = buildClient(metalake);
      ModelVersionChange change = ModelVersionChange.setProperty(property, value);
      if (version != null) {
        client.loadCatalog(catalog).asModelCatalog().alterModelVersion(name, version, change);
      } else {
        client.loadCatalog(catalog).asModelCatalog().alterModelVersion(name, alias, change);
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
      printInformation(model + " version " + alias + " property set.");
    } else {
      printInformation(model + " version " + version + " property set.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public Command validate() {
    validatePropertyAndValue(property, value);
    return super.validate();
  }
}
