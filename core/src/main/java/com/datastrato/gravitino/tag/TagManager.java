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
package com.datastrato.gravitino.tag;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.MetadataObject;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.exceptions.NoSuchTagException;
import com.datastrato.gravitino.exceptions.TagAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.TagEntity;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.storage.kv.KvEntityStore;
import com.datastrato.gravitino.utils.PrincipalUtils;
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
      LOG.warn(
          "TagManager cannot run with kv entity store, please configure the entity store to "
              + "use relational entity store and restart the Gravitino server");

      throw new RuntimeException(
          "TagManager cannot run with kv entity store, please configure "
              + "the entity store to use relational entity store and restart the Gravitino server");
    }

    this.idGenerator = idGenerator;
    this.entityStore = entityStore;
  }

  public String[] listTags(String metalake) {
    checkMetalakeExists(metalake, entityStore);

    try {
      return entityStore.list(ofTagNamespace(metalake), TagEntity.class, Entity.EntityType.TAG)
          .stream()
          .map(TagEntity::name)
          .toArray(String[]::new);
    } catch (IOException ioe) {
      LOG.error("Failed to list tags", ioe);
      throw new RuntimeException(ioe);
    }
  }

  public MetadataObject[] listAssociatedMetadataObjectsForTag(String metalake, String name) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public Tag createTag(String metalake, String name, String comment, Map<String, String> properties)
      throws TagAlreadyExistsException {
    Map<String, String> tagProperties = properties == null ? Collections.emptyMap() : properties;
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
      LOG.error("Failed to create tag", ioe);
      throw new RuntimeException(ioe);
    }
  }

  public Tag getTag(String metalake, String name) throws NoSuchTagException {
    checkMetalakeExists(metalake, entityStore);

    try {
      return entityStore.get(ofTagIdent(metalake, name), Entity.EntityType.TAG, TagEntity.class);
    } catch (NoSuchEntityException e) {
      throw new NoSuchTagException(
          "Tag with name %s under metalake %s does not exist", name, metalake);
    } catch (IOException ioe) {
      LOG.error("Failed to get tag", ioe);
      throw new RuntimeException(ioe);
    }
  }

  public Tag alterTag(String metalake, String name, TagChange... changes)
      throws NoSuchTagException, IllegalArgumentException {
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
      LOG.error("Failed to alter tag", ioe);
      throw new RuntimeException(ioe);
    }
  }

  public boolean deleteTag(String metalake, String name) {
    checkMetalakeExists(metalake, entityStore);

    try {
      return entityStore.delete(ofTagIdent(metalake, name), Entity.EntityType.TAG);
    } catch (IOException ioe) {
      LOG.error("Failed to delete tag", ioe);
      throw new RuntimeException(ioe);
    }
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
