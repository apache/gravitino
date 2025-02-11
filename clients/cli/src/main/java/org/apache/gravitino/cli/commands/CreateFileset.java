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
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.file.Fileset;

public class CreateFileset extends Command {
  protected final String metalake;
  protected final String catalog;
  protected final String schema;
  protected final String fileset;
  protected final String comment;
  protected final Map<String, String> properties;

  /**
   * Create a new fileset.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param fileset The name of the fileset.
   * @param comment The fileset's comment.
   * @param properties The catalog's properties.
   */
  public CreateFileset(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String fileset,
      String comment,
      Map<String, String> properties) {
    super(context);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.fileset = fileset;
    this.comment = comment;
    this.properties = properties;
  }

  /** Create a new fileset. */
  @Override
  public void handle() {
    NameIdentifier name = NameIdentifier.of(schema, fileset);
    boolean managed = properties.get("managed").equals("true");
    String location = properties.get("location");
    Fileset.Type filesetType = managed ? Fileset.Type.MANAGED : Fileset.Type.EXTERNAL;

    try {
      GravitinoClient client = buildClient(metalake);
      client
          .loadCatalog(catalog)
          .asFilesetCatalog()
          .createFileset(name, comment, filesetType, location, null);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchCatalogException err) {
      exitWithError(ErrorMessages.UNKNOWN_CATALOG);
    } catch (NoSuchSchemaException err) {
      exitWithError(ErrorMessages.UNKNOWN_SCHEMA);
    } catch (FilesetAlreadyExistsException err) {
      exitWithError(ErrorMessages.FILESET_EXISTS);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    printInformation(fileset + " created");
  }
}
