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

import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;

/** Remove a property of a schema. */
public class RemoveSchemaProperty extends Command {

  protected final String metalake;
  protected final String catalog;
  protected final String schema;
  protected final String property;

  /**
   * Remove a property of a schema.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param property The name of the property.
   */
  public RemoveSchemaProperty(
      CommandContext context, String metalake, String catalog, String schema, String property) {
    super(context);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.property = property;
  }

  /** Remove a property of a schema. */
  @Override
  public void handle() {
    try {
      GravitinoClient client = buildClient(metalake);
      SchemaChange change = SchemaChange.removeProperty(property);
      client.loadCatalog(catalog).asSchemas().alterSchema(schema, change);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchCatalogException err) {
      exitWithError(ErrorMessages.UNKNOWN_CATALOG);
    } catch (NoSuchSchemaException err) {
      exitWithError(ErrorMessages.UNKNOWN_SCHEMA);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    printInformation(property + " property removed.");
  }

  @Override
  public Command validate() {
    validateProperty(property);
    return super.validate();
  }
}
