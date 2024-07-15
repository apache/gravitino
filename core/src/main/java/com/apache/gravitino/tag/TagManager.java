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
package com.apache.gravitino.tag;

import com.apache.gravitino.Entity;
import com.apache.gravitino.EntityAlreadyExistsException;
import com.apache.gravitino.EntityStore;
import com.apache.gravitino.MetadataObject;
import com.apache.gravitino.NameIdentifier;
import com.apache.gravitino.Namespace;
import com.apache.gravitino.exceptions.NoSuchEntityException;
import com.apache.gravitino.exceptions.NoSuchMetalakeException;
import com.apache.gravitino.exceptions.NoSuchTagException;
import com.apache.gravitino.exceptions.TagAlreadyExistsException;
import com.apache.gravitino.lock.LockType;
import com.apache.gravitino.lock.TreeLockUtils;
import com.apache.gravitino.meta.AuditInfo;
import com.apache.gravitino.meta.TagEntity;
import com.apache.gravitino.storage.IdGenerator;
import com.apache.gravitino.storage.kv.KvEntityStore;
import com.apache.gravitino.utils.PrincipalUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TagManager {

  private static final Logger LOG = LoggerFactory.getLogger(TagManager.class);

  private final IdGenerator idGenerator;

  private final EntityStore entityStore;

  public TagManager(IdGenerator idGenerator, EntityStore entityStore) {
    if (entityStore instanceof KvEntityStore) {
      String errorMsg =
          "TagManager cannot run with kv entity store, please configure the entity "
              + "store to use relational entity store and restart the Gravitino server";
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }

    this.idGenerator = idGenerator;
    this.entityStore = entityStore;
  }

  public String[] listTags(String metalake) {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(ofTagNamespace(metalake).levels()),
        LockType.READ,
        () -> {
          checkMetalakeExists(metalake, entityStore);

          try {
            return entityStore
                .list(ofTagNamespace(metalake), TagEntity.class, Entity.EntityType.TAG).stream()
                .map(TagEntity::name)
                .toArray(String[]::new);
          } catch (IOException ioe) {
            LOG.error("Failed to list tags under metalake {}", metalake, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  public MetadataObject[] listAssociatedMetadataObjectsForTag(String metalake, String name) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public Tag createTag(String metalake, String name, String comment, Map<String, String> properties)
      throws TagAlreadyExistsException {
    Map<String, String> tagProperties = properties == null ? Collections.emptyMap() : properties;

    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(ofTagNamespace(metalake).levels()),
        LockType.WRITE,
        () -> {
          checkMetalakeExists(metalake, entityStore);

          TagEntity tagEntity =
              TagEntity.builder()
                  .withId(idGenerator.nextId())
                  .withName(name)
                  .withNamespace(ofTagNamespace(metalake))
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
    return TreeLockUtils.doWithTreeLock(
        ofTagIdent(metalake, name),
        LockType.READ,
        () -> {
          checkMetalakeExists(metalake, entityStore);

          try {
            return entityStore.get(
                ofTagIdent(metalake, name), Entity.EntityType.TAG, TagEntity.class);
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
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(ofTagNamespace(metalake).levels()),
        LockType.WRITE,
        () -> {
          checkMetalakeExists(metalake, entityStore);

          try {
            return entityStore.update(
                ofTagIdent(metalake, name),
                TagEntity.class,
                Entity.EntityType.TAG,
                tagEntity -> updateTagEntity(tagEntity, changes));
          } catch (NoSuchEntityException e) {
            throw new NoSuchTagException(
                "Tag with name %s under metalake %s does not exist", name, metalake);
          } catch (EntityAlreadyExistsException e) {
            throw new RuntimeException(
                "Tag with name " + name + " under metalake " + metalake + " already exists");
          } catch (IOException ioe) {
            LOG.error("Failed to alter tag {} under metalake {}", name, metalake, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  public boolean deleteTag(String metalake, String name) {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(ofTagNamespace(metalake).levels()),
        LockType.WRITE,
        () -> {
          checkMetalakeExists(metalake, entityStore);

          try {
            return entityStore.delete(ofTagIdent(metalake, name), Entity.EntityType.TAG);
          } catch (IOException ioe) {
            LOG.error("Failed to delete tag {} under metalake {}", name, metalake, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  public String[] listTagsForMetadataObject(String metalake, MetadataObject metadataObject) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public Tag[] listTagsInfoForMetadataObject(String metalake, MetadataObject metadataObject) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public Tag getTagForMetadataObject(String metalake, MetadataObject metadataObject, String name)
      throws NoSuchTagException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public String[] associateTagsForMetadataObject(
      String metalake, MetadataObject metadataObject, String[] tagsToAdd, String[] tagsToRemove) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  private static void checkMetalakeExists(String metalake, EntityStore entityStore) {
    try {
      NameIdentifier metalakeIdent = NameIdentifier.of(metalake);
      if (!entityStore.exists(metalakeIdent, Entity.EntityType.METALAKE)) {
        LOG.warn("Metalake {} does not exist", metalakeIdent);
        throw new NoSuchMetalakeException("Metalake %s does not exist", metalakeIdent);
      }
    } catch (IOException ioe) {
      LOG.error("Failed to check if metalake exists", ioe);
      throw new RuntimeException(ioe);
    }
  }

  @VisibleForTesting
  public static Namespace ofTagNamespace(String metalake) {
    return Namespace.of(metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.TAG_SCHEMA_NAME);
  }

  public static NameIdentifier ofTagIdent(String metalake, String tagName) {
    return NameIdentifier.of(ofTagNamespace(metalake), tagName);
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
