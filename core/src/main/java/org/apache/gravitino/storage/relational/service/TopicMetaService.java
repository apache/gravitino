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
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper;
import org.apache.gravitino.storage.relational.mapper.StatisticMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.po.TopicPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

/**
 * The service class for topic metadata. It provides the basic database operations for topic
 * metadata.
 */
public class TopicMetaService {
  private static final TopicMetaService INSTANCE = new TopicMetaService();

  public static TopicMetaService getInstance() {
    return INSTANCE;
  }

  private TopicMetaService() {}

  public void insertTopic(TopicEntity topicEntity, boolean overwrite) throws IOException {
    try {
      NameIdentifierUtil.checkTopic(topicEntity.nameIdentifier());

      TopicPO.Builder builder = TopicPO.builder();
      fillTopicPOBuilderParentEntityId(builder, topicEntity.namespace());

      SessionUtils.doWithCommit(
          TopicMetaMapper.class,
          mapper -> {
            TopicPO po = POConverters.initializeTopicPOWithVersion(topicEntity, builder);
            if (overwrite) {
              mapper.insertTopicMetaOnDuplicateKeyUpdate(po);
            } else {
              mapper.insertTopicMeta(po);
            }
          });
      // TODO: insert topic dataLayout version after supporting it
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.TOPIC, topicEntity.nameIdentifier().toString());
      throw re;
    }
  }

  public List<TopicEntity> listTopicsByNamespace(Namespace namespace) {
    NamespaceUtil.checkTopic(namespace);

    Long schemaId = CommonMetaService.getInstance().getParentEntityIdByNamespace(namespace);

    List<TopicPO> topicPOs =
        SessionUtils.getWithoutCommit(
            TopicMetaMapper.class, mapper -> mapper.listTopicPOsBySchemaId(schemaId));

    return POConverters.fromTopicPOs(topicPOs, namespace);
  }

  public <E extends Entity & HasIdentifier> TopicEntity updateTopic(
      NameIdentifier ident, Function<E, E> updater) throws IOException {
    NameIdentifierUtil.checkTopic(ident);

    String topicName = ident.name();

    Long schemaId = CommonMetaService.getInstance().getParentEntityIdByNamespace(ident.namespace());

    TopicPO oldTopicPO = getTopicPOBySchemaIdAndName(schemaId, topicName);
    TopicEntity oldTopicEntity = POConverters.fromTopicPO(oldTopicPO, ident.namespace());
    TopicEntity newEntity = (TopicEntity) updater.apply((E) oldTopicEntity);
    Preconditions.checkArgument(
        Objects.equals(oldTopicEntity.id(), newEntity.id()),
        "The updated topic entity id: %s should be same with the topic entity id before: %s",
        newEntity.id(),
        oldTopicEntity.id());

    Integer updateResult;
    try {
      updateResult =
          SessionUtils.doWithCommitAndFetchResult(
              TopicMetaMapper.class,
              mapper ->
                  mapper.updateTopicMeta(
                      POConverters.updateTopicPOWithVersion(oldTopicPO, newEntity), oldTopicPO));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.TOPIC, newEntity.nameIdentifier().toString());
      throw re;
    }

    if (updateResult > 0) {
      return newEntity;
    } else {
      throw new IOException("Failed to update the entity: " + ident);
    }
  }

  private TopicPO getTopicPOBySchemaIdAndName(Long schemaId, String topicName) {
    TopicPO topicPO =
        SessionUtils.getWithoutCommit(
            TopicMetaMapper.class,
            mapper -> mapper.selectTopicMetaBySchemaIdAndName(schemaId, topicName));

    if (topicPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.TOPIC.name().toLowerCase(),
          topicName);
    }
    return topicPO;
  }

  // Topic may be deleted, so the TopicPO may be null.
  public TopicPO getTopicPOById(Long topicId) {
    TopicPO topicPO =
        SessionUtils.getWithoutCommit(
            TopicMetaMapper.class, mapper -> mapper.selectTopicMetaById(topicId));
    return topicPO;
  }

  private void fillTopicPOBuilderParentEntityId(TopicPO.Builder builder, Namespace namespace) {
    NamespaceUtil.checkTopic(namespace);
    Long[] parentEntityIds =
        CommonMetaService.getInstance().getParentEntityIdsByNamespace(namespace);
    builder.withMetalakeId(parentEntityIds[0]);
    builder.withCatalogId(parentEntityIds[1]);
    builder.withSchemaId(parentEntityIds[2]);
  }

  public TopicEntity getTopicByIdentifier(NameIdentifier identifier) {
    NameIdentifierUtil.checkTopic(identifier);

    Long schemaId =
        CommonMetaService.getInstance().getParentEntityIdByNamespace(identifier.namespace());

    TopicPO topicPO = getTopicPOBySchemaIdAndName(schemaId, identifier.name());

    return POConverters.fromTopicPO(topicPO, identifier.namespace());
  }

  public boolean deleteTopic(NameIdentifier identifier) {
    NameIdentifierUtil.checkTopic(identifier);

    String topicName = identifier.name();

    Long schemaId =
        CommonMetaService.getInstance().getParentEntityIdByNamespace(identifier.namespace());

    Long topicId = getTopicIdBySchemaIdAndName(schemaId, topicName);

    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                TopicMetaMapper.class, mapper -> mapper.softDeleteTopicMetasByTopicId(topicId)),
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class,
                mapper ->
                    mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                        topicId, MetadataObject.Type.TOPIC.name())),
        () ->
            SessionUtils.doWithoutCommit(
                SecurableObjectMapper.class,
                mapper ->
                    mapper.softDeleteObjectRelsByMetadataObject(
                        topicId, MetadataObject.Type.TOPIC.name())),
        () ->
            SessionUtils.doWithoutCommit(
                TagMetadataObjectRelMapper.class,
                mapper ->
                    mapper.softDeleteTagMetadataObjectRelsByMetadataObject(
                        topicId, MetadataObject.Type.TOPIC.name())),
        () ->
            SessionUtils.doWithoutCommit(
                StatisticMetaMapper.class,
                mapper -> mapper.softDeleteStatisticsByEntityId(topicId)),
        () ->
            SessionUtils.doWithoutCommit(
                PolicyMetadataObjectRelMapper.class,
                mapper ->
                    mapper.softDeletePolicyMetadataObjectRelsByMetadataObject(
                        topicId, MetadataObject.Type.TOPIC.name())));

    return true;
  }

  public int deleteTopicMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        TopicMetaMapper.class,
        mapper -> {
          return mapper.deleteTopicMetasByLegacyTimeline(legacyTimeline, limit);
        });
  }

  public Long getTopicIdBySchemaIdAndName(Long schemaId, String topicName) {
    Long topicId =
        SessionUtils.getWithoutCommit(
            TopicMetaMapper.class,
            mapper -> mapper.selectTopicIdBySchemaIdAndName(schemaId, topicName));

    if (topicId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.TOPIC.name().toLowerCase(),
          topicName);
    }
    return topicId;
  }
}
