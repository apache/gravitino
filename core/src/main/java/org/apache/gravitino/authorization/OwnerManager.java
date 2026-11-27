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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.relational.service.GlossaryTermMetaService;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OwnerManager is used for manage the owner of metadata object. The user and group don't have an
 * owner. It takes no tree lock: the entity store keeps one live owner per object and fences the
 * object and the owner principal inside the owner write, which also lets the post hooks of create
 * operations call it without lock ordering concerns.
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
      OwnerImpl newOwner = new OwnerImpl(ownerName, ownerType, null);
      OwnerImpl originOwner;
      Long metadataId = null;
      if (metadataObject.type() == MetadataObject.Type.GLOSSARY_TERM) {
        // The owner route authorized a name. Recheck the current term and caller under the
        // term lock so a rename, recreation or transfer cannot change the assignment target.
        originOwner =
            GlossaryTermMetaService.getInstance()
                .withLockedTerm(
                    objectIdent,
                    false,
                    current -> {
                      if (current.isEmpty()) {
                        throw new NoSuchMetadataObjectException(
                            "Glossary term %s does not exist", objectIdent);
                      }
                      checkDefinitionOwnerTransfer(
                          metalake, metadataObject.type(), current.get().id());
                      return replaceOwner(metalake, metadataObject, objectIdent, newOwner);
                    });
      } else if (metadataObject.type() == MetadataObject.Type.KNOWLEDGE_BASE_ARTICLE) {
        record Transfer(long articleId, @Nullable OwnerImpl previous) {}
        Transfer transfer =
            KnowledgeBaseArticleMetaService.getInstance()
                .withLockedArticleId(
                    objectIdent,
                    id -> {
                      checkDefinitionOwnerTransfer(metalake, metadataObject.type(), id);
                      return new Transfer(
                          id, replaceOwner(metalake, metadataObject, objectIdent, newOwner));
                    });
        originOwner = transfer.previous();
        metadataId = transfer.articleId();
      } else {
        originOwner = replaceOwner(metalake, metadataObject, objectIdent, newOwner);
      }

      AuthorizationUtils.callAuthorizationPluginForMetadataObject(
          metalake,
          metadataObject,
          authorizationPlugin ->
              authorizationPlugin.onOwnerSet(metadataObject, originOwner, newOwner));
      notifyOwnerChange(originOwner, metalake, metadataObject, metadataId);
    } catch (NoSuchEntityException nse) {
      // The object existed when getOwner ran, so it can have been dropped since then. This
      // owner-exists check is a heuristic: if the owner is also removed between the failure and
      // this check, keep the generic NotFoundException; table creation can still fail in that race.
      if (ownerExists(metalake, ownerName, ownerType)) {
        throw new NoSuchMetadataObjectException(
            nse, "The metadata object of %s isn't found", metadataObject.fullName());
      }
      LOG.warn(
          "Metadata object {} or owner {} is not found", metadataObject.fullName(), ownerName, nse);
      throw new NotFoundException(
          nse, "Metadata object %s or owner %s is not found", metadataObject.fullName(), ownerName);
    }
  }

  @Override
  public void setOwners(
      String metalake,
      List<MetadataObject> metadataObjects,
      String ownerName,
      Owner.Type ownerType) {
    Preconditions.checkArgument(metadataObjects != null, "metadataObjects must not be null");
    if (metadataObjects.isEmpty()) {
      return;
    }

    Entity.EntityType objectType = MetadataObjectUtil.toEntityType(metadataObjects.get(0));
    for (int i = 1; i < metadataObjects.size(); i++) {
      Preconditions.checkArgument(
          MetadataObjectUtil.toEntityType(metadataObjects.get(i)) == objectType,
          "All metadata objects in setOwners must share the same type");
    }
    if (objectType == Entity.EntityType.GLOSSARY_TERM
        || objectType == Entity.EntityType.KNOWLEDGE_BASE_ARTICLE) {
      // Check each transfer while holding the corresponding definition's row lock.
      OwnerDispatcher.super.setOwners(metalake, metadataObjects, ownerName, ownerType);
      return;
    }

    List<OwnerImpl> originOwners = new ArrayList<>(metadataObjects.size());
    for (MetadataObject mo : metadataObjects) {
      originOwners.add(readOwner(metalake, mo));
    }

    List<NameIdentifier> owned =
        metadataObjects.stream()
            .map(mo -> MetadataObjectUtil.toEntityIdent(metalake, mo))
            .collect(Collectors.toList());

    OwnerImpl newOwner = new OwnerImpl(ownerName, ownerType, null);
    NameIdentifier ownerIdent;
    Entity.EntityType ownerEntityType;
    if (ownerType == Owner.Type.USER) {
      ownerIdent = AuthorizationUtils.ofUser(metalake, ownerName);
      ownerEntityType = Entity.EntityType.USER;
    } else if (ownerType == Owner.Type.GROUP) {
      ownerIdent = AuthorizationUtils.ofGroup(metalake, ownerName);
      ownerEntityType = Entity.EntityType.GROUP;
    } else {
      throw new IllegalArgumentException("Unsupported owner type: " + ownerType);
    }

    try {
      store
          .relationOperations()
          .batchInsertRelations(
              SupportsRelationOperations.Type.OWNER_REL,
              owned,
              objectType,
              ownerIdent,
              ownerEntityType,
              true);

      // Owner relations are already persisted; a failure for one plugin notification must not
      // abort the remaining notifications and leave the batch half-notified.
      for (int i = 0; i < metadataObjects.size(); i++) {
        MetadataObject mo = metadataObjects.get(i);
        OwnerImpl originOwner = originOwners.get(i);
        try {
          AuthorizationUtils.callAuthorizationPluginForMetadataObject(
              metalake,
              mo,
              authorizationPlugin -> authorizationPlugin.onOwnerSet(mo, originOwner, newOwner));
          notifyOwnerChange(originOwner, metalake, mo, null);
        } catch (RuntimeException re) {
          LOG.warn(
              "Failed to notify authorization plugin for metadata object {} during batch setOwners",
              mo.fullName(),
              re);
        }
      }
    } catch (NoSuchEntityException nse) {
      LOG.warn(
          "Metadata object or owner {} not found for batch setOwners: {}",
          ownerName,
          nse.getMessage());
      throw new NotFoundException(nse, "Metadata object or owner %s is not found", ownerName);
    } catch (IOException ioe) {
      LOG.warn(
          "Failed to batch set owner {} on {} objects", ownerName, metadataObjects.size(), ioe);
      throw new RuntimeException(ioe);
    }
  }

  private void notifyOwnerChange(
      @Nullable OwnerImpl oldOwner,
      String metalake,
      MetadataObject metadataObject,
      @Nullable Long metadataId) {
    GravitinoAuthorizer gravitinoAuthorizer = authorizer();
    if (gravitinoAuthorizer == null) {
      return;
    }

    Long oldOwnerId = oldOwner == null ? null : oldOwner.id;
    try {
      NameIdentifier ident = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
      Entity.EntityType type = MetadataObjectUtil.toEntityType(metadataObject);
      if (metadataId == null) {
        gravitinoAuthorizer.handleMetadataOwnerChange(metalake, oldOwnerId, ident, type);
      } else {
        gravitinoAuthorizer.handleMetadataOwnerChange(
            metalake, oldOwnerId, ident, type, metadataId);
      }
    } catch (RuntimeException e) {
      // Best-effort hook: a failing authorizer must not fail the owner-change operation itself.
      LOG.warn(
          "Authorizer hook failed for owner change on {} (metalake={}, oldOwnerId={})",
          metadataObject.fullName(),
          metalake,
          oldOwnerId,
          e);
    }
  }

  @Override
  public Optional<Owner> getOwner(String metalake, MetadataObject metadataObject) {
    GravitinoAuthorizer authorizer = authorizer();
    if ((metadataObject.type() == MetadataObject.Type.GLOSSARY_TERM
            || metadataObject.type() == MetadataObject.Type.KNOWLEDGE_BASE_ARTICLE)
        && authorizer != null) {
      // Visibility and the returned owner must refer to the same generation of this name.
      return SqlSessions.readSnapshot(
          () -> {
            Principal principal = PrincipalUtils.getCurrentPrincipal();
            AuthorizationRequestContext context = new AuthorizationRequestContext();
            boolean visible =
                metadataObject.type() == MetadataObject.Type.GLOSSARY_TERM
                    ? authorizer.canAccessGlossaryTerm(principal, metalake, metadataObject, context)
                    : authorizer.canAccessArticle(principal, metalake, metadataObject, context);
            if (!visible) {
              throw new ForbiddenException("Current user cannot get this object's owner");
            }
            return Optional.ofNullable(readOwner(metalake, metadataObject));
          });
    }
    return Optional.ofNullable(readOwner(metalake, metadataObject));
  }

  /**
   * Returns the authorizer, or null when this node runs without one. The server always installs an
   * authorizer, a pass-through one when authorization is disabled, so a null authorizer only
   * happens when {@code OwnerManager} is embedded without the authorization provider. Glossary
   * checks are then skipped, as {@link #notifyOwnerChange} skips cache invalidation.
   */
  @Nullable
  private static GravitinoAuthorizer authorizer() {
    return GravitinoEnv.getInstance().gravitinoAuthorizer();
  }

  @Nullable
  private OwnerImpl readOwner(String metalake, MetadataObject metadataObject) {
    NameIdentifier ident = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    try {
      List<? extends Entity> entities =
          store
              .relationOperations()
              .listEntitiesByRelation(
                  SupportsRelationOperations.Type.OWNER_REL,
                  ident,
                  MetadataObjectUtil.toEntityType(metadataObject));

      if (entities.isEmpty()) {
        return null;
      }
      if (entities.size() != 1) {
        throw new IllegalStateException(
            String.format("The size of the owner's name %s must be 1", metadataObject.fullName()));
      }
      Entity entity = entities.get(0);
      if (entity instanceof UserEntity user) {
        return new OwnerImpl(user.name(), Owner.Type.USER, user.id());
      }
      if (entity instanceof GroupEntity group) {
        return new OwnerImpl(group.name(), Owner.Type.GROUP, group.id());
      }
      throw new IllegalArgumentException(
          "Unsupported owner entity class: " + entity.getClass().getName());
    } catch (NoSuchEntityException nse) {
      throw new NoSuchMetadataObjectException(
          nse, "The metadata object of %s isn't found", metadataObject.fullName());
    } catch (IOException ioe) {
      LOG.info("Failed to get the owner of entity {}", metadataObject.fullName(), ioe);
      throw new RuntimeException(ioe);
    }
  }

  private void checkDefinitionOwnerTransfer(String metalake, MetadataObject.Type type, long id) {
    GravitinoAuthorizer authorizer = authorizer();
    if (authorizer == null) {
      return;
    }
    Principal principal = PrincipalUtils.getCurrentPrincipal();
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    MetadataObject lake = MetadataObjects.of(null, metalake, MetadataObject.Type.METALAKE);
    if (authorizer.isOwner(principal, metalake, lake, context)) {
      return;
    }
    boolean owns =
        type == MetadataObject.Type.GLOSSARY_TERM
            ? authorizer.isGlossaryTermOwner(principal, metalake, id, context)
            : authorizer.isArticleOwner(principal, metalake, id, context);
    if (!owns) {
      throw new ForbiddenException("Current user cannot set this object's owner");
    }
  }

  @Nullable
  private OwnerImpl replaceOwner(
      String metalake, MetadataObject object, NameIdentifier objectIdent, OwnerImpl newOwner) {
    OwnerImpl previous = readOwner(metalake, object);
    NameIdentifier ownerIdent;
    Entity.EntityType ownerType;
    if (newOwner.type == Owner.Type.USER) {
      ownerIdent = AuthorizationUtils.ofUser(metalake, newOwner.name);
      ownerType = Entity.EntityType.USER;
    } else if (newOwner.type == Owner.Type.GROUP) {
      ownerIdent = AuthorizationUtils.ofGroup(metalake, newOwner.name);
      ownerType = Entity.EntityType.GROUP;
    } else {
      throw new IllegalArgumentException("Unsupported owner type: " + newOwner.type);
    }
    try {
      store
          .relationOperations()
          .insertRelation(
              SupportsRelationOperations.Type.OWNER_REL,
              objectIdent,
              MetadataObjectUtil.toEntityType(object),
              ownerIdent,
              ownerType,
              true);
      return previous;
    } catch (IOException ioe) {
      LOG.info(
          "Failed to set the owner {} of metadata object {}",
          newOwner.name,
          object.fullName(),
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  private boolean ownerExists(String metalake, String ownerName, Owner.Type ownerType) {
    try {
      return ownerType == Owner.Type.USER
          ? store.exists(AuthorizationUtils.ofUser(metalake, ownerName), Entity.EntityType.USER)
          : store.exists(AuthorizationUtils.ofGroup(metalake, ownerName), Entity.EntityType.GROUP);
    } catch (IOException ioe) {
      LOG.warn("Failed to check whether owner {} exists", ownerName, ioe);
      return false;
    }
  }

  private static final class OwnerImpl implements Owner {
    private final String name;
    private final Type type;
    @Nullable private final Long id;

    private OwnerImpl(String name, Type type, @Nullable Long id) {
      this.name = name;
      this.type = type;
      this.id = id;
    }

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
