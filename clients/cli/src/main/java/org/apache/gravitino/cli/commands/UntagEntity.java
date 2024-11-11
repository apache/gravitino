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
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.cli.FullName;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.Table;

public class UntagEntity extends Command {
  protected final String metalake;
  protected final FullName name;
  protected final String tag;

  /**
   * Untag an entity with an existing tag.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param name The name of the entity.
   * @param tag The name of the tag.
   */
  public UntagEntity(
      String url, boolean ignoreVersions, String metalake, FullName name, String tag) {
    super(url, ignoreVersions);
    this.metalake = metalake;
    this.name = name;
    this.tag = tag;
  }

  /** Create a new tag. */
  @Override
  public void handle() {
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
        tags = gTable.supportsTags().associateTags(null, new String[] {tag});
        entity = table;
      } else if (name.hasSchemaName()) {
        String catalog = name.getCatalogName();
        String schema = name.getSchemaName();
        Schema gSchema = client.loadCatalog(catalog).asSchemas().loadSchema(schema);
        tags = gSchema.supportsTags().associateTags(null, new String[] {tag});
        entity = schema;
      } else if (name.hasCatalogName()) {
        String catalog = name.getCatalogName();
        Catalog gCatalog = client.loadCatalog(catalog);
        tags = gCatalog.supportsTags().associateTags(null, new String[] {tag});
        entity = catalog;
      }
    } catch (NoSuchMetalakeException err) {
      System.err.println(ErrorMessages.UNKNOWN_METALAKE);
      return;
    } catch (NoSuchCatalogException err) {
      System.err.println(ErrorMessages.UNKNOWN_CATALOG);
      return;
    } catch (NoSuchSchemaException err) {
      System.err.println(ErrorMessages.UNKNOWN_TABLE);
      return;
    } catch (NoSuchTableException err) {
      System.err.println(ErrorMessages.UNKNOWN_TABLE);
      return;
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
      return;
    }

    String all = String.join(",", tags);

<<<<<<< HEAD
    System.out.println(entity + " untagged, tagged with " + all);
=======
    if (all.equals("")) {
      all = "nothing";
    }

    System.out.println(entity + " removed tag " + tag + ", now tagged with " + all);
>>>>>>> main
  }
}
