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
package org.apache.gravitino.authorization;

import java.io.IOException;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.OwnerNotFoundException;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.relation.Relation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OwnershipManager {
  private static final Logger LOG = LoggerFactory.getLogger(OwnershipManager.class);
  private final EntityStore store;

  public OwnershipManager(EntityStore store) {
    this.store = store;
  }

  public void setOwner(
      NameIdentifier identifier, Entity.EntityType type, String ownerName, Owner.Type ownerType) {
    try {
      String metalake;
      if (type == Entity.EntityType.METALAKE) {
        metalake = identifier.name();
      } else {
        metalake = identifier.namespace().level(0);
      }

      if (ownerType == Owner.Type.USER) {
        store
            .relationOperations()
            .insertRelation(
                Relation.Type.OWNER_REL,
                identifier,
                type,
                AuthorizationUtils.ofUser(metalake, ownerName),
                Entity.EntityType.USER,
                true);
      } else if (ownerType == Owner.Type.GROUP) {
        store
            .relationOperations()
            .insertRelation(
                Relation.Type.OWNER_REL,
                identifier,
                type,
                AuthorizationUtils.ofGroup(metalake, ownerName),
                Entity.EntityType.GROUP,
                true);
      }
    } catch (NoSuchEntityException nse) {
      LOG.warn("Entity {} or owner {} is not found", identifier, ownerName, nse);
      throw new NotFoundException(nse, "Entity %s or owner %s is not found", identifier, ownerName);
    } catch (IOException ioe) {
      LOG.info("Fail to set the owner {} of entity {}", ownerName, identifier, ioe);
      throw new RuntimeException(ioe);
    }
  }

  public Owner getOwner(NameIdentifier identifier, Entity.EntityType type) {
    try {
      OwnerImpl owner = new OwnerImpl();
      List entities =
          store
              .relationOperations()
              .listEntitiesByRelation(Relation.Type.OWNER_REL, identifier, type);
      if (entities.isEmpty()) {
        throw new OwnerNotFoundException("The owner of %s isn't found", identifier.toString());
      }

      if (entities.size() != 1) {
        throw new IllegalStateException(
            String.format("The number of the owner %s must be 1", identifier.toString()));
      }

      if (entities.get(0) instanceof UserEntity) {
        UserEntity user = (UserEntity) entities.get(0);
        owner.name = user.name();
        owner.type = Owner.Type.USER;
      } else if (entities.get(0) instanceof GroupEntity) {
        GroupEntity group = (GroupEntity) entities.get(0);
        owner.name = group.name();
        owner.type = Owner.Type.GROUP;
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Doesn't support owner entity class %s", entities.get(0).getClass().getName()));
      }

      return owner;
    } catch (NoSuchEntityException nse) {
      throw new OwnerNotFoundException("The owner of %s isn't found", identifier.toString());
    } catch (IOException ioe) {
      LOG.info("Fail to get the owner of entity {}", identifier, ioe);
      throw new RuntimeException(ioe);
    }
  }

  private static class OwnerImpl implements Owner {

    private String name;
    private Type type;

    @Override
    public String name() {
      return name;
    }

    @Override
    public Type type() {
      return type;
    }
  }
}
