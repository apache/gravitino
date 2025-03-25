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
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.FullName;
import org.apache.gravitino.client.GravitinoClient;

public class MetadataCommand extends Command {

  /**
   * MetadataCommand constructor.
   *
   * @param context The command context.
   */
  public MetadataCommand(CommandContext context) {
    super(context);
  }

  /**
   * Constructs a {@link MetadataObject} based on the provided client, existing metadata object, and
   * entity name.
   *
   * @param entity The name of the entity.
   * @param client The Gravitino client.
   * @return A MetadataObject of the appropriate type.
   * @throws IllegalArgumentException if the entity type cannot be determined or is unknown.
   */
  protected MetadataObject constructMetadataObject(FullName entity, GravitinoClient client) {

    MetadataObject metadataObject;
    String name = entity.getName();

    if (entity.hasColumnName()) {
      metadataObject = MetadataObjects.of(null, name, MetadataObject.Type.COLUMN);
    } else if (entity.hasTableName()) {
      Catalog catalog = client.loadCatalog(entity.getCatalogName());
      Catalog.Type catalogType = catalog.type();
      if (catalogType == Catalog.Type.RELATIONAL) {
        metadataObject = MetadataObjects.of(null, name, MetadataObject.Type.TABLE);
      } else if (catalogType == Catalog.Type.MESSAGING) {
        metadataObject = MetadataObjects.of(null, name, MetadataObject.Type.TOPIC);
      } else if (catalogType == Catalog.Type.FILESET) {
        metadataObject = MetadataObjects.of(null, name, MetadataObject.Type.FILESET);
      } else {
        throw new IllegalArgumentException("Unknown entity type: " + name);
      }
    } else if (entity.hasSchemaName()) {
      metadataObject = MetadataObjects.of(null, name, MetadataObject.Type.SCHEMA);
    } else if (entity.hasCatalogName()) {
      metadataObject = MetadataObjects.of(null, name, MetadataObject.Type.CATALOG);
    } else if (entity.getMetalakeName() != null) {
      metadataObject = MetadataObjects.of(null, name, MetadataObject.Type.METALAKE);
    } else {
      throw new IllegalArgumentException("Unknown entity type: " + name);
    }
    return metadataObject;
  }

  /* Do nothing, as parent will override. */
  @Override
  public void handle() {}
}
