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
import org.apache.gravitino.exceptions.TagAlreadyAssociatedException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.rel.Table;

public class TagEntity extends Command {
  protected final String metalake;
  protected final FullName name;
  protected final String[] tags;

  /**
   * Tag an entity with existing tags.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param name The name of the entity.
   * @param tags The names of the tags.
   */
  public TagEntity(CommandContext context, String metalake, FullName name, String[] tags) {
    super(context);
    this.metalake = metalake;
    this.name = name;
    this.tags = tags;
  }

  /** Add tags for an entity. */
  @Override
  public void handle() {
    String entity = "unknown";
    String[] tagsToAdd = new String[0];

    try {
      GravitinoClient client = buildClient(metalake);

      if (name.getLevel() == 3) {
        String catalog = name.getCatalogName();
        Catalog catalogObject = client.loadCatalog(catalog);
        switch (catalogObject.type()) {
          case RELATIONAL:
            String table = name.getTableName();
            entity = table;
            Table gTable = catalogObject.asTableCatalog().loadTable(FullNameUtil.toTable(name));
            tagsToAdd = gTable.supportsTags().associateTags(tags, null);
            break;

          case MODEL:
            String model = name.getModelName();
            entity = model;
            Model gModel = catalogObject.asModelCatalog().getModel(FullNameUtil.toModel(name));
            tagsToAdd = gModel.supportsTags().associateTags(tags, null);
            break;

          case FILESET:
            String fileset = name.getFilesetName();
            entity = fileset;
            Fileset gFileset =
                catalogObject.asFilesetCatalog().loadFileset(FullNameUtil.toFileset(name));
            gFileset.supportsTags().associateTags(tags, null);
            break;

          case MESSAGING:
            String topic = name.getTopicName();
            entity = topic;
            Topic gTopic = catalogObject.asTopicCatalog().loadTopic(FullNameUtil.toTopic(name));
            gTopic.supportsTags().associateTags(tags, null);
            break;

          default:
            break;
        }
      } else if (name.hasSchemaName()) {
        String catalog = name.getCatalogName();
        String schema = name.getSchemaName();
        Schema gSchema = client.loadCatalog(catalog).asSchemas().loadSchema(schema);
        tagsToAdd = gSchema.supportsTags().associateTags(tags, null);
        entity = schema;
      } else if (name.hasCatalogName()) {
        String catalog = name.getCatalogName();
        Catalog gCatalog = client.loadCatalog(catalog);
        tagsToAdd = gCatalog.supportsTags().associateTags(tags, null);
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
    } catch (TagAlreadyAssociatedException err) {
      exitWithError("Tags are already associated with " + name.getName());
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    String all = tagsToAdd.length == 0 ? "nothing" : String.join(",", tagsToAdd);

    printInformation(entity + " now tagged with " + all);
  }

  @Override
  public Command validate() {
    if (name == null || !name.hasName()) exitWithError(ErrorMessages.MISSING_NAME);
    return super.validate();
  }
}
