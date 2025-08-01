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
package org.apache.gravitino.tag;

import static org.apache.gravitino.metalake.MetalakeManager.checkMetalake;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.TagAlreadyAssociatedException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TagManager implements TagDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(TagManager.class);

  private final IdGenerator idGenerator;

  private final EntityStore entityStore;

  private final SupportsTagOperations supportsTagOperations;

  public TagManager(IdGenerator idGenerator, EntityStore entityStore) {
    if (!(entityStore instanceof SupportsTagOperations)) {
      String errorMsg =
          "TagManager cannot run with entity store that does not support tag operations, "
              + "please configure the entity store to use relational entity store and restart the Gravitino server";
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }

    this.supportsTagOperations = entityStore.tagOperations();

    this.idGenerator = idGenerator;
    this.entityStore = entityStore;
  }

  public String[] listTags(String metalake) {
    return Arrays.stream(listTagsInfo(metalake)).map(Tag::name).toArray(String[]::new);
  }

  public Tag[] listTagsInfo(String metalake) {
    checkMetalake(NameIdentifier.of(metalake), entityStore);
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(NamespaceUtil.ofTag(metalake).levels()),
        LockType.READ,
        () -> {
          try {
            return entityStore
                .list(NamespaceUtil.ofTag(metalake), TagEntity.class, Entity.EntityType.TAG)
                .stream()
                .toArray(Tag[]::new);
          } catch (IOException ioe) {
            LOG.error("Failed to list tags under metalake {}", metalake, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  public Tag createTag(String metalake, String name, String comment, Map<String, String> properties)
      throws TagAlreadyExistsException {
    Map<String, String> tagProperties = properties == null ? Collections.emptyMap() : properties;
    checkMetalake(NameIdentifier.of(metalake), entityStore);

    return TreeLockUtils.doWithTreeLock(
        NameIdentifierUtil.ofTag(metalake, name),
        LockType.WRITE,
        () -> {
          TagEntity tagEntity =
              TagEntity.builder()
                  .withId(idGenerator.nextId())
                  .withName(name)
                  .withNamespace(NamespaceUtil.ofTag(metalake))
                  .withComment(comment)
                  .withProperties(tagProperties)
                  .withAuditInfo(
                      AuditInfo.builder()
                          .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                          .withCreateTime(Instant.now())
                          .build())
                  .build();

          try {
            entityStore.put(tagEntity, false /* overwritten */);
            return tagEntity;
          } catch (EntityAlreadyExistsException e) {
            throw new TagAlreadyExistsException(
                "Tag with name %s under metalake %s already exists", name, metalake);
          } catch (IOException ioe) {
            LOG.error("Failed to create tag {} under metalake {}", name, metalake, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  public Tag getTag(String metalake, String name) throws NoSuchTagException {
    checkMetalake(NameIdentifier.of(metalake), entityStore);
    return TreeLockUtils.doWithTreeLock(
        NameIdentifierUtil.ofTag(metalake, name),
        LockType.READ,
        () -> {
          try {
            return entityStore.get(
                NameIdentifierUtil.ofTag(metalake, name), Entity.EntityType.TAG, TagEntity.class);
          } catch (NoSuchEntityException e) {
            throw new NoSuchTagException(
                "Tag with name %s under metalake %s does not exist", name, metalake);
          } catch (IOException ioe) {
            LOG.error("Failed to get tag {} under metalake {}", name, metalake, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  public Tag alterTag(String metalake, String name, TagChange... changes)
      throws NoSuchTagException, IllegalArgumentException {
    checkMetalake(NameIdentifier.of(metalake), entityStore);
    return TreeLockUtils.doWithTreeLock(
        NameIdentifierUtil.ofTag(metalake, name),
        LockType.WRITE,
        () -> {
          try {
            return entityStore.update(
                NameIdentifierUtil.ofTag(metalake, name),
                TagEntity.class,
                Entity.EntityType.TAG,
                tagEntity -> updateTagEntity(tagEntity, changes));
          } catch (NoSuchEntityException e) {
            throw new NoSuchTagException(
                "Tag with name %s under metalake %s does not exist", name, metalake);
          } catch (EntityAlreadyExistsException e) {
            throw new RuntimeException(
                String.format(
                    "Trying to alter tag %s under metalake %s, but the new name already exists",
                    name, metalake));
          } catch (IOException ioe) {
            LOG.error("Failed to alter tag {} under metalake {}", name, metalake, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  public boolean deleteTag(String metalake, String name) {
    checkMetalake(NameIdentifier.of(metalake), entityStore);
    return TreeLockUtils.doWithTreeLock(
        NameIdentifierUtil.ofTag(metalake, name),
        LockType.WRITE,
        () -> {
          try {
            return entityStore.delete(
                NameIdentifierUtil.ofTag(metalake, name), Entity.EntityType.TAG);
          } catch (IOException ioe) {
            LOG.error("Failed to delete tag {} under metalake {}", name, metalake, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  public MetadataObject[] listMetadataObjectsForTag(String metalake, String name)
      throws NoSuchTagException {
    NameIdentifier tagId = NameIdentifierUtil.ofTag(metalake, name);
    checkMetalake(NameIdentifier.of(metalake), entityStore);
    return TreeLockUtils.doWithTreeLock(
        tagId,
        LockType.READ,
        () -> {
          try {
            if (!entityStore.exists(tagId, Entity.EntityType.TAG)) {
              throw new NoSuchTagException(
                  "Tag with name %s under metalake %s does not exist", name, metalake);
            }

            return supportsTagOperations
                .listAssociatedMetadataObjectsForTag(tagId)
                .toArray(new MetadataObject[0]);
          } catch (IOException e) {
            LOG.error("Failed to list metadata objects for tag {}", name, e);
            throw new RuntimeException(e);
          }
        });
  }

  public String[] listTagsForMetadataObject(String metalake, MetadataObject metadataObject)
      throws NotFoundException {
    return Arrays.stream(listTagsInfoForMetadataObject(metalake, metadataObject))
        .map(Tag::name)
        .toArray(String[]::new);
  }

  public Tag[] listTagsInfoForMetadataObject(String metalake, MetadataObject metadataObject)
      throws NoSuchMetadataObjectException {
    NameIdentifier entityIdent = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    Entity.EntityType entityType = MetadataObjectUtil.toEntityType(metadataObject);

    MetadataObjectUtil.checkMetadataObject(metalake, metadataObject);

    return TreeLockUtils.doWithTreeLock(
        entityIdent,
        LockType.READ,
        () -> {
          try {
            checkMetalake(NameIdentifier.of(metalake), entityStore);
            return supportsTagOperations
                .listAssociatedTagsForMetadataObject(entityIdent, entityType)
                .toArray(new Tag[0]);
          } catch (NoSuchEntityException e) {
            throw new NoSuchMetadataObjectException(
                e, "Failed to list tags for metadata object %s due to not found", metadataObject);
          } catch (IOException e) {
            LOG.error("Failed to list tags for metadata object {}", metadataObject, e);
            throw new RuntimeException(e);
          }
        });
  }

  public Tag getTagForMetadataObject(String metalake, MetadataObject metadataObject, String name)
      throws NoSuchMetadataObjectException {
    NameIdentifier entityIdent = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    Entity.EntityType entityType = MetadataObjectUtil.toEntityType(metadataObject);
    NameIdentifier tagIdent = NameIdentifierUtil.ofTag(metalake, name);

    MetadataObjectUtil.checkMetadataObject(metalake, metadataObject);

    return TreeLockUtils.doWithTreeLock(
        entityIdent,
        LockType.READ,
        () -> {
          try {
            checkMetalake(NameIdentifier.of(metalake), entityStore);
            return supportsTagOperations.getTagForMetadataObject(entityIdent, entityType, tagIdent);
          } catch (NoSuchEntityException e) {
            if (e.getMessage().contains("No such tag entity")) {
              throw new NoSuchTagException(
                  e, "Tag %s does not exist for metadata object %s", name, metadataObject);
            } else {
              throw new NoSuchMetadataObjectException(
                  e, "Failed to get tag for metadata object %s due to not found", metadataObject);
            }
          } catch (IOException e) {
            LOG.error("Failed to get tag for metadata object {}", metadataObject, e);
            throw new RuntimeException(e);
          }
        });
  }

  public String[] associateTagsForMetadataObject(
      String metalake, MetadataObject metadataObject, String[] tagsToAdd, String[] tagsToRemove)
      throws NoSuchMetadataObjectException, TagAlreadyAssociatedException {
    Preconditions.checkArgument(
        !metadataObject.type().equals(MetadataObject.Type.METALAKE)
            && !metadataObject.type().equals(MetadataObject.Type.ROLE),
        "Cannot associate tags for unsupported metadata object type %s",
        metadataObject.type());

    NameIdentifier entityIdent = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    Entity.EntityType entityType = MetadataObjectUtil.toEntityType(metadataObject);

    MetadataObjectUtil.checkMetadataObject(metalake, metadataObject);

    // Remove all the tags that are both set to add and remove
    Set<String> tagsToAddSet = tagsToAdd == null ? Sets.newHashSet() : Sets.newHashSet(tagsToAdd);
    Set<String> tagsToRemoveSet =
        tagsToRemove == null ? Sets.newHashSet() : Sets.newHashSet(tagsToRemove);
    Set<String> common = Sets.intersection(tagsToAddSet, tagsToRemoveSet).immutableCopy();
    tagsToAddSet.removeAll(common);
    tagsToRemoveSet.removeAll(common);

    NameIdentifier[] tagsToAddIdent =
        tagsToAddSet.stream()
            .map(tag -> NameIdentifierUtil.ofTag(metalake, tag))
            .toArray(NameIdentifier[]::new);
    NameIdentifier[] tagsToRemoveIdent =
        tagsToRemoveSet.stream()
            .map(tag -> NameIdentifierUtil.ofTag(metalake, tag))
            .toArray(NameIdentifier[]::new);

    return TreeLockUtils.doWithTreeLock(
        entityIdent,
        LockType.READ,
        () ->
            TreeLockUtils.doWithTreeLock(
                NameIdentifier.of(NamespaceUtil.ofTag(metalake).levels()),
                LockType.WRITE,
                () -> {
                  try {
                    return supportsTagOperations
                        .associateTagsWithMetadataObject(
                            entityIdent, entityType, tagsToAddIdent, tagsToRemoveIdent)
                        .stream()
                        .map(Tag::name)
                        .toArray(String[]::new);
                  } catch (NoSuchEntityException e) {
                    throw new NoSuchMetadataObjectException(
                        e,
                        "Failed to associate tags for metadata object %s due to not found",
                        metadataObject);
                  } catch (EntityAlreadyExistsException e) {
                    throw new TagAlreadyAssociatedException(
                        e,
                        "Failed to associate tags for metadata object due to some tags %s already "
                            + "associated to the metadata object %s",
                        Arrays.toString(tagsToAdd),
                        metadataObject);
                  } catch (IOException e) {
                    LOG.error("Failed to associate tags for metadata object {}", metadataObject, e);
                    throw new RuntimeException(e);
                  }
                }));
  }

  private TagEntity updateTagEntity(TagEntity tagEntity, TagChange... changes) {
    Map<String, String> props =
        tagEntity.properties() == null
            ? Maps.newHashMap()
            : Maps.newHashMap(tagEntity.properties());
    String newName = tagEntity.name();
    String newComment = tagEntity.comment();

    for (TagChange change : changes) {
      if (change instanceof TagChange.RenameTag) {
        newName = ((TagChange.RenameTag) change).getNewName();
      } else if (change instanceof TagChange.UpdateTagComment) {
        newComment = ((TagChange.UpdateTagComment) change).getNewComment();
      } else if (change instanceof TagChange.SetProperty) {
        TagChange.SetProperty setProperty = (TagChange.SetProperty) change;
        props.put(setProperty.getProperty(), setProperty.getValue());
      } else if (change instanceof TagChange.RemoveProperty) {
        TagChange.RemoveProperty removeProperty = (TagChange.RemoveProperty) change;
        props.remove(removeProperty.getProperty());
      } else {
        throw new IllegalArgumentException("Unsupported tag change: " + change);
      }
    }

    return TagEntity.builder()
        .withId(tagEntity.id())
        .withName(newName)
        .withNamespace(tagEntity.namespace())
        .withComment(newComment)
        .withProperties(props)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(tagEntity.auditInfo().creator())
                .withCreateTime(tagEntity.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withLastModifiedTime(Instant.now())
                .build())
        .build();
  }
}
