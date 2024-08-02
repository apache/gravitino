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
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.relation.Relation;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** OwnershipManager is used for manage the owner of securable objects, user, group and role. */
public class OwnershipManager {
  private static final Logger LOG = LoggerFactory.getLogger(OwnershipManager.class);
  private final EntityStore store;

  public OwnershipManager(EntityStore store) {
    this.store = store;
  }

  public void setOwner(
      String metalake, MetadataObject metadataObject, String ownerName, Owner.Type ownerType) {
    try {
      if (ownerType == Owner.Type.USER) {
        store
            .relationOperations()
            .insertRelation(
                Relation.Type.OWNER_REL,
                MetadataObjectUtil.toEntityIdent(metalake, metadataObject),
                MetadataObjectUtil.toEntityType(metadataObject),
                AuthorizationUtils.ofUser(metalake, ownerName),
                Entity.EntityType.USER,
                true);
      } else if (ownerType == Owner.Type.GROUP) {
        store
            .relationOperations()
            .insertRelation(
                Relation.Type.OWNER_REL,
                MetadataObjectUtil.toEntityIdent(metalake, metadataObject),
                MetadataObjectUtil.toEntityType(metadataObject),
                AuthorizationUtils.ofGroup(metalake, ownerName),
                Entity.EntityType.GROUP,
                true);
      }
    } catch (NoSuchEntityException nse) {
      LOG.warn(
          "Metadata object {} or owner {} is not found", metadataObject.fullName(), ownerName, nse);
      throw new NotFoundException(
          nse, "Metadata object %s or owner %s is not found", metadataObject.fullName(), ownerName);
    } catch (IOException ioe) {
      LOG.info(
          "Fail to set the owner {} of metadata object {}",
          ownerName,
          metadataObject.fullName(),
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  public Optional<Owner> getOwner(String metalake, MetadataObject metadataObject) {
    try {
      OwnerImpl owner = new OwnerImpl();
      List entities =
          store
              .relationOperations()
              .listEntitiesByRelation(
                  Relation.Type.OWNER_REL,
                  MetadataObjectUtil.toEntityIdent(metalake, metadataObject),
                  MetadataObjectUtil.toEntityType(metadataObject));

      if (entities.isEmpty()) {
        return Optional.empty();
      }

      if (entities.size() != 1) {
        throw new IllegalStateException(
            String.format("The number of the owner %s must be 1", metadataObject.fullName()));
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

      return Optional.of(owner);
    } catch (NoSuchEntityException nse) {
      throw new NotFoundException(
          "The metadata object of %s isn't found", metadataObject.fullName());
    } catch (IOException ioe) {
      LOG.info("Fail to get the owner of entity {}", metadataObject.fullName(), ioe);
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
