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
  protected final boolean managed;
  protected final String location;

  /**
   * Create a new fileset.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param fileset The name of the fileset.
   * @param comment The fileset's comment.
   * @param managed True if it is a managed fileset, false if it is an external fileset.
   * @param location Location of the fileset.
   */
  public CreateFileset(
      String url,
      boolean ignoreVersions,
      String metalake,
      String catalog,
      String schema,
      String fileset,
      String comment,
      boolean managed,
      String location) {
    super(url, ignoreVersions);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.fileset = fileset;
    this.comment = comment;
    this.managed = managed;
    this.location = location;
  }

  /** Create a new fileset. */
  public void handle() {
    NameIdentifier name = NameIdentifier.of(schema, fileset);
    Fileset.Type filesetType = managed ? Fileset.Type.MANAGED : Fileset.Type.EXTERNAL;

    try {
      GravitinoClient client = buildClient(metalake);
      client
          .loadCatalog(catalog)
          .asFilesetCatalog()
          .createFileset(name, comment, filesetType, location, null);
    } catch (NoSuchMetalakeException err) {
      System.err.println(ErrorMessages.UNKNOWN_METALAKE);
      return;
    } catch (NoSuchCatalogException err) {
      System.err.println(ErrorMessages.UNKNOWN_CATALOG);
      return;
    } catch (NoSuchSchemaException err) {
      System.err.println(ErrorMessages.UNKNOWN_SCHEMA);
      return;
    } catch (FilesetAlreadyExistsException err) {
      System.err.println(ErrorMessages.FILESET_EXISTS);
      return;
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
      return;
    }

    System.out.println(fileset + " created");
  }
}
