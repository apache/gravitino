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
import org.apache.gravitino.Schema;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.cli.FullName;
import org.apache.gravitino.cli.utils.FullNameUtil;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.rel.Table;

/* Lists all tags in a metalake. */
public class ListEntityTags extends Command {

  protected String metalake;
  protected FullName name;

  /**
   * Lists all tags in a metalake.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param name The name of the entity.
   */
  public ListEntityTags(CommandContext context, String metalake, FullName name) {
    super(context);
    this.metalake = metalake;
    this.name = name;
  }

  /** Lists all tags in a metalake. */
  @Override
  public void handle() {
    String[] tags = new String[0];
    try {
      GravitinoClient client = buildClient(metalake);

      if (name.getLevel() == 3) {
        String catalog = name.getCatalogName();
        Catalog catalogObject = client.loadCatalog(catalog);
        switch (catalogObject.type()) {
          case RELATIONAL:
            Table gTable = catalogObject.asTableCatalog().loadTable(FullNameUtil.toTable(name));
            tags = gTable.supportsTags().listTags();
            break;

          case MODEL:
            Model gModel = catalogObject.asModelCatalog().getModel(FullNameUtil.toModel(name));
            tags = gModel.supportsTags().listTags();
            break;

          case FILESET:
            Fileset fileset =
                catalogObject.asFilesetCatalog().loadFileset(FullNameUtil.toFileset(name));
            tags = fileset.supportsTags().listTags();
            break;

          case MESSAGING:
            Topic topic = catalogObject.asTopicCatalog().loadTopic(FullNameUtil.toTopic(name));
            tags = topic.supportsTags().listTags();
            break;

          default:
            break;
        }
      } else if (name.hasSchemaName()) {
        String catalog = name.getCatalogName();
        String schema = name.getSchemaName();
        Schema gSchema = client.loadCatalog(catalog).asSchemas().loadSchema(schema);
        tags = gSchema.supportsTags().listTags();
      } else if (name.hasCatalogName()) {
        String catalog = name.getCatalogName();
        Catalog gCatalog = client.loadCatalog(catalog);
        tags = gCatalog.supportsTags().listTags();
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

    String all = String.join(",", tags);

    printResults(all);
  }
}
