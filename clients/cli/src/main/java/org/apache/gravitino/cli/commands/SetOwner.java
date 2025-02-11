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

import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.CommandEntities;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

public class SetOwner extends Command {

  protected final String metalake;
  protected final String entity;
  protected final MetadataObject.Type entityType;
  protected final String owner;
  protected final boolean isGroup;

  /**
   * Sets the owner of an entity.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param entity The name of the entity.
   * @param entityType The type entity.
   * @param owner The name of the new owner.
   * @param isGroup True if the owner is a group, false if it is not.
   */
  public SetOwner(
      CommandContext context,
      String metalake,
      String entity,
      String entityType,
      String owner,
      boolean isGroup) {
    super(context);
    this.metalake = metalake;
    this.entity = entity;
    this.owner = owner;
    this.isGroup = isGroup;

    if (entityType.equals(CommandEntities.METALAKE)) {
      this.entityType = MetadataObject.Type.METALAKE;
    } else if (entityType.equals(CommandEntities.CATALOG)) {
      this.entityType = MetadataObject.Type.CATALOG;
    } else if (entityType.equals(CommandEntities.SCHEMA)) {
      this.entityType = MetadataObject.Type.SCHEMA;
    } else if (entityType.equals(CommandEntities.TABLE)) {
      this.entityType = MetadataObject.Type.TABLE;
    } else if (entityType.equals(CommandEntities.COLUMN)) {
      this.entityType = MetadataObject.Type.COLUMN;
    } else if (entityType.equals(CommandEntities.TOPIC)) {
      this.entityType = MetadataObject.Type.TOPIC;
    } else if (entityType.equals(CommandEntities.FILESET)) {
      this.entityType = MetadataObject.Type.FILESET;
    } else {
      this.entityType = null;
    }
  }

  /** Sets the owner of an entity. */
  @Override
  public void handle() {
    MetadataObject metadata = MetadataObjects.parse(entity, entityType);
    Owner.Type ownerType = isGroup ? Owner.Type.GROUP : Owner.Type.USER;

    try {
      GravitinoClient client = buildClient(metalake);
      client.setOwner(metadata, owner, ownerType);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchMetadataObjectException err) {
      exitWithError(ErrorMessages.UNKNOWN_ENTITY);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    printInformation("Set owner to " + owner);
  }
}
