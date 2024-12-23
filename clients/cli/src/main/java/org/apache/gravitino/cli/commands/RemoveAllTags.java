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

import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.cli.AreYouSure;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.cli.FullName;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.Table;

/* Removes all the tags of an entity. */
public class RemoveAllTags extends Command {

  protected String metalake;
  protected FullName name;
  protected final boolean force;

  /**
   * Removes all the tags of an entity
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param name The name of the entity.
   * @param force Force operation.
   */
  public RemoveAllTags(
      String url, boolean ignoreVersions, String metalake, FullName name, boolean force) {
    super(url, ignoreVersions);
    this.metalake = metalake;
    this.name = name;
    this.force = force;
  }

  @Override
  public void handle() {
    if (!AreYouSure.really(force)) {
      return;
    }
    String entity = "unknown";
    String[] tags = new String[0];
    try {

      GravitinoClient client = buildClient(metalake);
      // TODO fileset and topic
      if (name.hasTableName()) {
        String catalog = name.getCatalogName();
        String schema = name.getSchemaName();
        String table = name.getTableName();
        Table gTable =
            client
                .loadCatalog(catalog)
                .asTableCatalog()
                .loadTable(NameIdentifier.of(schema, table));
        tags = gTable.supportsTags().listTags();
        if (tags.length > 0) {
          gTable.supportsTags().associateTags(null, tags);
        }
        entity = table;
      } else if (name.hasSchemaName()) {
        String catalog = name.getCatalogName();
        String schema = name.getSchemaName();
        Schema gSchema = client.loadCatalog(catalog).asSchemas().loadSchema(schema);
        tags = gSchema.supportsTags().listTags();
        if (tags.length > 0) {
          gSchema.supportsTags().associateTags(null, tags);
        }
        entity = schema;
      } else if (name.hasCatalogName()) {
        String catalog = name.getCatalogName();
        Catalog gCatalog = client.loadCatalog(catalog);
        tags = gCatalog.supportsTags().listTags();
        if (tags.length > 0) {
          gCatalog.supportsTags().associateTags(null, tags);
        }
        entity = catalog;
      }
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchCatalogException err) {
      exitWithError(ErrorMessages.UNKNOWN_CATALOG);
    } catch (NoSuchSchemaException err) {
      exitWithError(ErrorMessages.UNKNOWN_SCHEMA);
    } catch (NoSuchTableException err) {
      exitWithError(ErrorMessages.UNKNOWN_TABLE);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    if (tags.length > 0) {
      System.out.println(
          entity + " removed tags " + String.join(",", tags) + " now tagged with nothing");
    } else {
      System.out.println(entity + " has no tags");
    }
  }
}
