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
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param entity The name of the entity.
   * @param entityType The type entity.
   * @param owner The name of the new owner.
   * @param isGroup True if the owner is a group, false if it is not.
   */
  public SetOwner(
      String url,
      boolean ignoreVersions,
      String metalake,
      String entity,
      String entityType,
      String owner,
      boolean isGroup) {
    super(url, ignoreVersions);
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
      System.err.println(ErrorMessages.UNKNOWN_METALAKE);
      return;
    } catch (NoSuchMetadataObjectException err) {
      System.err.println(ErrorMessages.UNKNOWN_ENTITY);
      return;
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
      return;
    }

    System.out.println("Set owner to " + owner);
  }
}
