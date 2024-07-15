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
package com.datastrato.gravitino.storage.relational.service;

import com.apache.gravitino.NameIdentifier;
import com.apache.gravitino.Namespace;
import com.apache.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.meta.TagEntity;
import com.datastrato.gravitino.storage.relational.mapper.TagMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import com.datastrato.gravitino.storage.relational.po.TagPO;
import com.datastrato.gravitino.storage.relational.utils.ExceptionUtils;
import com.datastrato.gravitino.storage.relational.utils.POConverters;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

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
                SessionUtils.doWithoutCommitAndFetchResult(
                    TagMetaMapper.class,
                    mapper ->
                        mapper.softDeleteTagMetaByMetalakeAndTagName(metalakeName, ident.name())),
        () ->
            tagMetadataObjectRelDeletedCount[0] =
                SessionUtils.doWithoutCommitAndFetchResult(
                    TagMetadataObjectRelMapper.class,
                    mapper ->
                        mapper.softDeleteTagMetadataObjectRelsByMetalakeAndTagName(
                            metalakeName, ident.name())));

    return tagDeletedCount[0] + tagMetadataObjectRelDeletedCount[0] > 0;
  }

  public int deleteTagMetasByLegacyTimeline(long legacyTimeline, int limit) {
    int[] tagDeletedCount = new int[] {0};
    int[] tagMetadataObjectRelDeletedCount = new int[] {0};

    SessionUtils.doMultipleWithCommit(
        () ->
            tagDeletedCount[0] =
                SessionUtils.doWithoutCommitAndFetchResult(
                    TagMetaMapper.class,
                    mapper -> mapper.deleteTagMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            tagMetadataObjectRelDeletedCount[0] =
                SessionUtils.doWithoutCommitAndFetchResult(
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
}
