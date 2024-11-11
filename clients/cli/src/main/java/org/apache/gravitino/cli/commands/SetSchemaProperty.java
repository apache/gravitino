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
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;

/** Set a property of a schema. */
public class SetSchemaProperty extends Command {

  protected final String metalake;
  protected final String catalog;
  protected final String schema;
  protected final String property;
  protected final String value;

  /**
   * Set a property of a schema.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param property The name of the property.
   * @param value The value of the property.
   */
  public SetSchemaProperty(
      String url,
      boolean ignoreVersions,
      String metalake,
      String catalog,
      String schema,
      String property,
      String value) {
    super(url, ignoreVersions);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.property = property;
    this.value = value;
  }

  /** Set a property of a schema. */
  @Override
  public void handle() {
    try {
      GravitinoClient client = buildClient(metalake);
      SchemaChange change = SchemaChange.setProperty(property, value);
      client.loadCatalog(catalog).asSchemas().alterSchema(schema, change);
    } catch (NoSuchMetalakeException err) {
      System.err.println(ErrorMessages.UNKNOWN_METALAKE);
      return;
    } catch (NoSuchCatalogException err) {
      System.err.println(ErrorMessages.UNKNOWN_CATALOG);
      return;
    } catch (NoSuchSchemaException err) {
      System.err.println(ErrorMessages.UNKNOWN_SCHEMA);
      return;
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
      return;
    }

    System.out.println(schema + " property set.");
  }
}
