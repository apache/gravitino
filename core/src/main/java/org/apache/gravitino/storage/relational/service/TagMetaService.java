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
package org.apache.gravitino.storage.relational.service;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.storage.relational.mapper.TagMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.po.TagMetadataObjectRelPO;
import org.apache.gravitino.storage.relational.po.TagPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

public class TagMetaService {

  private static final TagMetaService INSTANCE = new TagMetaService();

  public static TagMetaService getInstance() {
    return INSTANCE;
  }

  private TagMetaService() {}

  public List<TagEntity> listTagsByNamespace(Namespace ns) {
    String metalakeName = ns.level(0);
    List<TagPO> tagPOs =
        SessionUtils.getWithoutCommit(
            TagMetaMapper.class, mapper -> mapper.listTagPOsByMetalake(metalakeName));
    return tagPOs.stream()
        .map(tagPO -> POConverters.fromTagPO(tagPO, ns))
        .collect(Collectors.toList());
  }

  public TagEntity getTagByIdentifier(NameIdentifier ident) {
    String metalakeName = ident.namespace().level(0);
    TagPO tagPO = getTagPOByMetalakeAndName(metalakeName, ident.name());
    return POConverters.fromTagPO(tagPO, ident.namespace());
  }

  public void insertTag(TagEntity tagEntity, boolean overwritten) throws IOException {
    Namespace ns = tagEntity.namespace();
    String metalakeName = ns.level(0);

    try {
      Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalakeName);

      TagPO.Builder builder = TagPO.builder().withMetalakeId(metalakeId);
      TagPO tagPO = POConverters.initializeTagPOWithVersion(tagEntity, builder);

      SessionUtils.doWithCommit(
          TagMetaMapper.class,
          mapper -> {
            if (overwritten) {
              mapper.insertTagMetaOnDuplicateKeyUpdate(tagPO);
            } else {
              mapper.insertTagMeta(tagPO);
            }
          });
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.TAG, tagEntity.toString());
      throw e;
    }
  }

  public <E extends Entity & HasIdentifier> TagEntity updateTag(
      NameIdentifier ident, Function<E, E> updater) throws IOException {
    String metalakeName = ident.namespace().level(0);

    try {
      TagPO tagPO = getTagPOByMetalakeAndName(metalakeName, ident.name());
      TagEntity oldTagEntity = POConverters.fromTagPO(tagPO, ident.namespace());
      TagEntity updatedTagEntity = (TagEntity) updater.apply((E) oldTagEntity);
      Preconditions.checkArgument(
          Objects.equals(oldTagEntity.id(), updatedTagEntity.id()),
          "The updated tag entity id: %s must have the same id as the old entity id %s",
          updatedTagEntity.id(),
          oldTagEntity.id());

      Integer result =
          SessionUtils.doWithCommitAndFetchResult(
              TagMetaMapper.class,
              mapper ->
                  mapper.updateTagMeta(
                      POConverters.updateTagPOWithVersion(tagPO, updatedTagEntity), tagPO));

      if (result == null || result == 0) {
        throw new IOException("Failed to update the entity: " + ident);
      }

      return updatedTagEntity;

    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.TAG, ident.toString());
      throw e;
    }
  }

  public boolean deleteTag(NameIdentifier ident) {
    String metalakeName = ident.namespace().level(0);
    int[] tagDeletedCount = new int[] {0};
    int[] tagMetadataObjectRelDeletedCount = new int[] {0};

    SessionUtils.doMultipleWithCommit(
        () ->
            tagDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    TagMetaMapper.class,
                    mapper ->
                        mapper.softDeleteTagMetaByMetalakeAndTagName(metalakeName, ident.name())),
        () ->
            tagMetadataObjectRelDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    TagMetadataObjectRelMapper.class,
                    mapper ->
                        mapper.softDeleteTagMetadataObjectRelsByMetalakeAndTagName(
                            metalakeName, ident.name())));

    return tagDeletedCount[0] + tagMetadataObjectRelDeletedCount[0] > 0;
  }

  public List<TagEntity> listTagsForMetadataObject(
      NameIdentifier objectIdent, Entity.EntityType objectType)
      throws NoSuchTagException, IOException {
    MetadataObject metadataObject = NameIdentifierUtil.toMetadataObject(objectIdent, objectType);
    String metalake = objectIdent.namespace().level(0);

    List<TagPO> tagPOs = null;
    try {
      Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
      Long metadataObjectId =
          MetadataObjectService.getMetadataObjectId(
              metalakeId, metadataObject.fullName(), metadataObject.type());

      tagPOs =
          SessionUtils.getWithoutCommit(
              TagMetadataObjectRelMapper.class,
              mapper ->
                  mapper.listTagPOsByMetadataObjectIdAndType(
                      metadataObjectId, metadataObject.type().toString()));
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.TAG, objectIdent.toString());
      throw e;
    }

    return tagPOs.stream()
        .map(tagPO -> POConverters.fromTagPO(tagPO, NamespaceUtil.ofTag(metalake)))
        .collect(Collectors.toList());
  }

  public TagEntity getTagForMetadataObject(
      NameIdentifier objectIdent, Entity.EntityType objectType, NameIdentifier tagIdent)
      throws NoSuchEntityException, IOException {
    MetadataObject metadataObject = NameIdentifierUtil.toMetadataObject(objectIdent, objectType);
    String metalake = objectIdent.namespace().level(0);

    TagPO tagPO = null;
    try {
      Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
      Long metadataObjectId =
          MetadataObjectService.getMetadataObjectId(
              metalakeId, metadataObject.fullName(), metadataObject.type());

      tagPO =
          SessionUtils.getWithoutCommit(
              TagMetadataObjectRelMapper.class,
              mapper ->
                  mapper.getTagPOsByMetadataObjectAndTagName(
                      metadataObjectId, metadataObject.type().toString(), tagIdent.name()));
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.TAG, tagIdent.toString());
      throw e;
    }

    if (tagPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.TAG.name().toLowerCase(),
          tagIdent.name());
    }

    return POConverters.fromTagPO(tagPO, NamespaceUtil.ofTag(metalake));
  }

  public List<MetadataObject> listAssociatedMetadataObjectsForTag(NameIdentifier tagIdent)
      throws IOException {
    String metalakeName = tagIdent.namespace().level(0);
    String tagName = tagIdent.name();

    try {
      List<TagMetadataObjectRelPO> tagMetadataObjectRelPOs =
          SessionUtils.doWithCommitAndFetchResult(
              TagMetadataObjectRelMapper.class,
              mapper ->
                  mapper.listTagMetadataObjectRelsByMetalakeAndTagName(metalakeName, tagName));

      List<MetadataObject> metadataObjects = Lists.newArrayList();
      Map<String, List<TagMetadataObjectRelPO>> tagMetadataObjectRelPOsByType =
          tagMetadataObjectRelPOs.stream()
              .collect(Collectors.groupingBy(TagMetadataObjectRelPO::getMetadataObjectType));

      for (Map.Entry<String, List<TagMetadataObjectRelPO>> entry :
          tagMetadataObjectRelPOsByType.entrySet()) {
        String metadataObjectType = entry.getKey();
        List<TagMetadataObjectRelPO> rels = entry.getValue();

        List<Long> metadataObjectIds =
            rels.stream()
                .map(TagMetadataObjectRelPO::getMetadataObjectId)
                .collect(Collectors.toList());
        Map<Long, String> metadataObjectNames =
            MetadataObjectService.TYPE_TO_FULLNAME_FUNCTION_MAP
                .get(MetadataObject.Type.valueOf(metadataObjectType))
                .apply(metadataObjectIds);

        for (Map.Entry<Long, String> metadataObjectName : metadataObjectNames.entrySet()) {
          String fullName = metadataObjectName.getValue();

          // Metadata object may be deleted asynchronously when we query the name, so it will
          // return null, we should skip this metadata object.
          if (fullName != null) {
            metadataObjects.add(
                MetadataObjects.parse(fullName, MetadataObject.Type.valueOf(metadataObjectType)));
          }
        }
      }

      return metadataObjects;
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.TAG, tagIdent.toString());
      throw e;
    }
  }

  public List<TagEntity> associateTagsWithMetadataObject(
      NameIdentifier objectIdent,
      Entity.EntityType objectType,
      NameIdentifier[] tagsToAdd,
      NameIdentifier[] tagsToRemove)
      throws NoSuchEntityException, EntityAlreadyExistsException, IOException {
    MetadataObject metadataObject = NameIdentifierUtil.toMetadataObject(objectIdent, objectType);
    String metalake = objectIdent.namespace().level(0);

    try {
      Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
      Long metadataObjectId =
          MetadataObjectService.getMetadataObjectId(
              metalakeId, metadataObject.fullName(), metadataObject.type());

      // Fetch all the tags need to associate with the metadata object.
      List<String> tagNamesToAdd =
          Arrays.stream(tagsToAdd).map(NameIdentifier::name).collect(Collectors.toList());
      List<TagPO> tagPOsToAdd =
          tagNamesToAdd.isEmpty()
              ? Collections.emptyList()
              : getTagPOsByMetalakeAndNames(metalake, tagNamesToAdd);

      // Fetch all the tags need to remove from the metadata object.
      List<String> tagNamesToRemove =
          Arrays.stream(tagsToRemove).map(NameIdentifier::name).collect(Collectors.toList());
      List<TagPO> tagPOsToRemove =
          tagNamesToRemove.isEmpty()
              ? Collections.emptyList()
              : getTagPOsByMetalakeAndNames(metalake, tagNamesToRemove);

      SessionUtils.doMultipleWithCommit(
          () -> {
            // Insert the tag metadata object relations.
            if (tagPOsToAdd.isEmpty()) {
              return;
            }

            List<TagMetadataObjectRelPO> tagRelsToAdd =
                tagPOsToAdd.stream()
                    .map(
                        tagPO ->
                            POConverters.initializeTagMetadataObjectRelPOWithVersion(
                                tagPO.getTagId(),
                                metadataObjectId,
                                metadataObject.type().toString()))
                    .collect(Collectors.toList());
            SessionUtils.doWithoutCommit(
                TagMetadataObjectRelMapper.class,
                mapper -> mapper.batchInsertTagMetadataObjectRels(tagRelsToAdd));
          },
          () -> {
            // Remove the tag metadata object relations.
            if (tagPOsToRemove.isEmpty()) {
              return;
            }

            List<Long> tagIdsToRemove =
                tagPOsToRemove.stream().map(TagPO::getTagId).collect(Collectors.toList());
            SessionUtils.doWithoutCommit(
                TagMetadataObjectRelMapper.class,
                mapper ->
                    mapper.batchDeleteTagMetadataObjectRelsByTagIdsAndMetadataObject(
                        metadataObjectId, metadataObject.type().toString(), tagIdsToRemove));
          });

      // Fetch all the tags associated with the metadata object after the operation.
      List<TagPO> tagPOs =
          SessionUtils.getWithoutCommit(
              TagMetadataObjectRelMapper.class,
              mapper ->
                  mapper.listTagPOsByMetadataObjectIdAndType(
                      metadataObjectId, metadataObject.type().toString()));

      return tagPOs.stream()
          .map(tagPO -> POConverters.fromTagPO(tagPO, NamespaceUtil.ofTag(metalake)))
          .collect(Collectors.toList());

    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.TAG, objectIdent.toString());
      throw e;
    }
  }

  public int deleteTagMetasByLegacyTimeline(long legacyTimeline, int limit) {
    int[] tagDeletedCount = new int[] {0};
    int[] tagMetadataObjectRelDeletedCount = new int[] {0};

    SessionUtils.doMultipleWithCommit(
        () ->
            tagDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    TagMetaMapper.class,
                    mapper -> mapper.deleteTagMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            tagMetadataObjectRelDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    TagMetadataObjectRelMapper.class,
                    mapper -> mapper.deleteTagEntityRelsByLegacyTimeline(legacyTimeline, limit)));

    return tagDeletedCount[0] + tagMetadataObjectRelDeletedCount[0];
  }

  private TagPO getTagPOByMetalakeAndName(String metalakeName, String tagName) {
    TagPO tagPO =
        SessionUtils.getWithoutCommit(
            TagMetaMapper.class,
            mapper -> mapper.selectTagMetaByMetalakeAndName(metalakeName, tagName));

    if (tagPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.TAG.name().toLowerCase(),
          tagName);
    }
    return tagPO;
  }

  private List<TagPO> getTagPOsByMetalakeAndNames(String metalakeName, List<String> tagNames) {
    return SessionUtils.getWithoutCommit(
        TagMetaMapper.class,
        mapper -> mapper.listTagPOsByMetalakeAndTagNames(metalakeName, tagNames));
  }
}
