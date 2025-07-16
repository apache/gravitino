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
import lombok.Getter;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OwnerManager is used for manage the owner of metadata object. The user and group don't have an
 * owner. Because the post hook will call the methods. We shouldn't add the lock of the metadata
 * object. Otherwise, it will cause deadlock.
 */
public class OwnerManager implements OwnerDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(OwnerManager.class);
  @Getter private final EntityStore store;

  public OwnerManager(EntityStore store) {
    if (store instanceof SupportsRelationOperations) {
      this.store = store;
    } else {
      String errorMsg =
          "OwnerManager currently only supports relational entity store, "
              + "please configure the entity store to use relational entity store and restart the Gravitino server";
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }
  }

  @Override
  public void setOwner(
      String metalake, MetadataObject metadataObject, String ownerName, Owner.Type ownerType) {
    NameIdentifier objectIdent = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    try {
      Optional<Owner> originOwner = getOwner(metalake, metadataObject);

      OwnerImpl newOwner = new OwnerImpl();
      if (ownerType == Owner.Type.USER) {
        NameIdentifier ownerIdent = AuthorizationUtils.ofUser(metalake, ownerName);
        TreeLockUtils.doWithTreeLock(
            ownerIdent,
            LockType.READ,
            () -> {
              store
                  .relationOperations()
                  .insertRelation(
                      SupportsRelationOperations.Type.OWNER_REL,
                      objectIdent,
                      MetadataObjectUtil.toEntityType(metadataObject),
                      ownerIdent,
                      Entity.EntityType.USER,
                      true);
              return null;
            });

        newOwner.name = ownerName;
        newOwner.type = Owner.Type.USER;
      } else if (ownerType == Owner.Type.GROUP) {
        NameIdentifier ownerIdent = AuthorizationUtils.ofGroup(metalake, ownerName);
        TreeLockUtils.doWithTreeLock(
            ownerIdent,
            LockType.READ,
            () -> {
              store
                  .relationOperations()
                  .insertRelation(
                      SupportsRelationOperations.Type.OWNER_REL,
                      objectIdent,
                      MetadataObjectUtil.toEntityType(metadataObject),
                      ownerIdent,
                      Entity.EntityType.GROUP,
                      true);
              return null;
            });

        newOwner.name = ownerName;
        newOwner.type = Owner.Type.GROUP;
      }

      AuthorizationUtils.callAuthorizationPluginForMetadataObject(
          metalake,
          metadataObject,
          authorizationPlugin ->
              authorizationPlugin.onOwnerSet(metadataObject, originOwner.orElse(null), newOwner));
      originOwner.ifPresent(owner -> notifyOwnerChange(owner, metalake, metadataObject));
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

  private void notifyOwnerChange(Owner oldOwner, String metalake, MetadataObject metadataObject) {
    GravitinoAuthorizer gravitinoAuthorizer = GravitinoEnv.getInstance().gravitinoAuthorizer();
    if (gravitinoAuthorizer != null) {
      if (oldOwner.type() == Owner.Type.USER) {
        try {
          UserEntity userEntity =
              GravitinoEnv.getInstance()
                  .entityStore()
                  .get(
                      NameIdentifierUtil.ofUser(metalake, oldOwner.name()),
                      Entity.EntityType.USER,
                      UserEntity.class);
          gravitinoAuthorizer.handleMetadataOwnerChange(
              metalake,
              userEntity.id(),
              MetadataObjectUtil.toEntityIdent(metalake, metadataObject),
              Entity.EntityType.valueOf(metadataObject.type().name()));
        } catch (IOException e) {
          LOG.warn(e.getMessage(), e);
        }
      } else {
        throw new UnsupportedOperationException(
            "Notification for Group Owner is not supported yet.");
      }
    }
  }

  @Override
  public Optional<Owner> getOwner(String metalake, MetadataObject metadataObject) {
    NameIdentifier ident = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    OwnerImpl owner = new OwnerImpl();
    try {
      List<? extends Entity> entities =
          TreeLockUtils.doWithTreeLock(
              ident,
              LockType.READ,
              () ->
                  store
                      .relationOperations()
                      .listEntitiesByRelation(
                          SupportsRelationOperations.Type.OWNER_REL,
                          ident,
                          MetadataObjectUtil.toEntityType(metadataObject)));

      if (entities.isEmpty()) {
        return Optional.empty();
      }

      if (entities.size() != 1) {
        throw new IllegalStateException(
            String.format("The size of the owner's name %s must be 1", metadataObject.fullName()));
      }

      Entity entity = entities.get(0);
      if (!(entity instanceof UserEntity) && !(entity instanceof GroupEntity)) {
        throw new IllegalArgumentException(
            String.format(
                "Doesn't support owner entity class %s", entities.get(0).getClass().getName()));
      }

      if (entities.get(0) instanceof UserEntity) {
        UserEntity user = (UserEntity) entities.get(0);
        owner.name = user.name();
        owner.type = Owner.Type.USER;
      } else if (entities.get(0) instanceof GroupEntity) {
        GroupEntity group = (GroupEntity) entities.get(0);
        owner.name = group.name();
        owner.type = Owner.Type.GROUP;
      }
      return Optional.of(owner);
    } catch (NoSuchEntityException nse) {
      throw new NoSuchMetadataObjectException(
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
