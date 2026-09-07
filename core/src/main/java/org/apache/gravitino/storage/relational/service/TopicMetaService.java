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

import static org.apache.gravitino.metrics.source.MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.metrics.Monitored;
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

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "insertTopic")
  public void insertTopic(TopicEntity topicEntity, boolean overwrite) throws IOException {
    try {
      NameIdentifierUtil.checkTopic(topicEntity.nameIdentifier());

      TopicPO.Builder builder = TopicPO.builder();
      fillTopicPOBuilderParentEntityId(builder, topicEntity.namespace());
      TopicPO po = POConverters.initializeTopicPOWithVersion(topicEntity, builder);

      SessionUtils.doMultipleWithCommit(
          // Hold the parent schema row until this transaction ends, so the topic cannot be
          // written below a schema that is being dropped.
          () ->
              SchemaMetaService.getInstance()
                  .lockSchemaForEntityWrite(
                      topicEntity.nameIdentifier(),
                      po.getSchemaId(),
                      po.getCatalogId(),
                      po.getMetalakeId()),
          () ->
              SessionUtils.doWithoutCommit(
                  TopicMetaMapper.class,
                  mapper -> {
                    if (overwrite) {
                      mapper.insertTopicMetaOnDuplicateKeyUpdate(po);
                    } else {
                      mapper.insertTopicMeta(po);
                    }
                  }));
      // TODO: insert topic dataLayout version after supporting it
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.TOPIC, topicEntity.nameIdentifier().toString());
      throw re;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listTopicsByNamespace")
  public List<TopicEntity> listTopicsByNamespace(Namespace namespace) {
    NamespaceUtil.checkTopic(namespace);

    List<TopicPO> topicPOs = listTopicPOs(namespace);
    return POConverters.fromTopicPOs(topicPOs, namespace);
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "updateTopic")
  public <E extends Entity & HasIdentifier> TopicEntity updateTopic(
      NameIdentifier ident, Function<E, E> updater) throws IOException {
    TopicPO oldTopicPO = getTopicPOByIdentifier(ident);
    TopicEntity oldTopicEntity = POConverters.fromTopicPO(oldTopicPO, ident.namespace());
    TopicEntity newEntity = (TopicEntity) updater.apply((E) oldTopicEntity);
    Preconditions.checkArgument(
        Objects.equals(oldTopicEntity.id(), newEntity.id()),
        "The updated topic entity id: %s should be same with the topic entity id before: %s",
        newEntity.id(),
        oldTopicEntity.id());

    try {
      TopicPO newTopicPO = POConverters.updateTopicPOWithVersion(oldTopicPO, newEntity);
      SessionUtils.doMultipleWithCommit(
          () -> {
            // current_version is the decision point for the whole write. Even if another writer
            // changes the payload and later restores it, that writer still advances the version,
            // so this stale update changes zero rows.
            int updated =
                SessionUtils.getWithoutCommit(
                    TopicMetaMapper.class,
                    mapper -> mapper.updateTopicMeta(newTopicPO, oldTopicPO));
            if (updated == 0) {
              throw topicWriteFailure(ident, oldTopicPO);
            }
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.TOPIC, newEntity.nameIdentifier().toString());
      throw re;
    }

    return newEntity;
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

  private TopicPO getTopicPOByIdentifier(NameIdentifier identifier) {
    NameIdentifierUtil.checkTopic(identifier);

    return topicPOFetcher().apply(identifier);
  }

  private List<TopicPO> listTopicPOs(Namespace namespace) {
    return topicListFetcher().apply(namespace);
  }

  private List<TopicPO> listTopicPOsBySchemaId(Namespace namespace) {
    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(namespace.levels()), Entity.EntityType.SCHEMA);

    return SessionUtils.getWithoutCommit(
        TopicMetaMapper.class, mapper -> mapper.listTopicPOsBySchemaId(schemaId));
  }

  private List<TopicPO> listTopicPOsByFullQualifiedName(Namespace namespace) {
    if (namespace == null || namespace.length() != 3) {
      throw new NoSuchEntityException(
          "Topic namespace must have 3 levels, the input namespace is %s", namespace);
    }
    String[] namespaceLevels = namespace.levels();
    List<TopicPO> topicPOs =
        SessionUtils.getWithoutCommit(
            TopicMetaMapper.class,
            mapper ->
                mapper.listTopicPOsByFullQualifiedName(
                    namespaceLevels[0], namespaceLevels[1], namespaceLevels[2]));
    if (topicPOs.isEmpty() || topicPOs.get(0).getSchemaId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          namespaceLevels[2]);
    }
    return topicPOs.stream().filter(po -> po.getTopicId() != null).collect(Collectors.toList());
  }

  private TopicPO getTopicPOBySchemaId(NameIdentifier identifier) {
    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(identifier.namespace().levels()), Entity.EntityType.SCHEMA);
    return getTopicPOBySchemaIdAndName(schemaId, identifier.name());
  }

  private TopicPO getTopicPOByFullQualifiedName(NameIdentifier identifier) {
    if (identifier == null
        || identifier.namespace() == null
        || identifier.namespace().length() != 3) {
      throw new NoSuchEntityException(
          "Topic identifier must have a 3-level namespace, the input identifier is %s", identifier);
    }
    String[] namespaceLevels = identifier.namespace().levels();
    TopicPO topicPO =
        SessionUtils.getWithoutCommit(
            TopicMetaMapper.class,
            mapper ->
                mapper.selectTopicByFullQualifiedName(
                    namespaceLevels[0], namespaceLevels[1], namespaceLevels[2], identifier.name()));

    if (topicPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.TOPIC.name().toLowerCase(),
          identifier.name());
    }

    if (topicPO.getSchemaId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          namespaceLevels[2]);
    }

    if (topicPO.getTopicId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.TOPIC.name().toLowerCase(),
          identifier.name());
    }

    return topicPO;
  }

  private Function<Namespace, List<TopicPO>> topicListFetcher() {
    return GravitinoEnv.getInstance().cacheEnabled()
        ? this::listTopicPOsBySchemaId
        : this::listTopicPOsByFullQualifiedName;
  }

  private Function<NameIdentifier, TopicPO> topicPOFetcher() {
    return GravitinoEnv.getInstance().cacheEnabled()
        ? this::getTopicPOBySchemaId
        : this::getTopicPOByFullQualifiedName;
  }

  private void fillTopicPOBuilderParentEntityId(TopicPO.Builder builder, Namespace namespace) {
    NamespaceUtil.checkTopic(namespace);
    NamespacedEntityId namespacedEntityId =
        EntityIdService.getEntityIds(
            NameIdentifier.of(namespace.levels()), Entity.EntityType.SCHEMA);
    builder.withMetalakeId(namespacedEntityId.namespaceIds()[0]);
    builder.withCatalogId(namespacedEntityId.namespaceIds()[1]);
    builder.withSchemaId(namespacedEntityId.entityId());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getTopicByIdentifier")
  public TopicEntity getTopicByIdentifier(NameIdentifier identifier) {
    TopicPO topicPO = getTopicPOByIdentifier(identifier);
    return POConverters.fromTopicPO(topicPO, identifier.namespace());
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "deleteTopic")
  public boolean deleteTopic(NameIdentifier identifier) {
    TopicPO topicPO = getTopicPOByIdentifier(identifier);
    deleteTopicWithVersion(identifier, topicPO);
    return true;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteTopicMetasByLegacyTimeline")
  public int deleteTopicMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        TopicMetaMapper.class,
        mapper -> {
          return mapper.deleteTopicMetasByLegacyTimeline(legacyTimeline, limit);
        });
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getTopicIdBySchemaIdAndName")
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

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchGetTopicByIdentifier")
  public List<TopicEntity> batchGetTopicByIdentifier(List<NameIdentifier> identifiers) {
    NameIdentifier firstIdent = identifiers.get(0);
    NameIdentifier schemaIdent = NameIdentifierUtil.getSchemaIdentifier(firstIdent);
    List<String> topicNames =
        identifiers.stream().map(NameIdentifier::name).collect(Collectors.toList());

    return SessionUtils.doWithCommitAndFetchResult(
        TopicMetaMapper.class,
        mapper -> {
          List<TopicPO> topicPOs =
              mapper.batchSelectTopicByIdentifier(
                  schemaIdent.namespace().level(0),
                  schemaIdent.namespace().level(1),
                  schemaIdent.name(),
                  topicNames);
          return POConverters.fromTopicPOs(topicPOs, firstIdent.namespace());
        });
  }

  /**
   * Deletes the observed topic and its dependent rows in one transaction.
   *
   * <p>Package access lets concurrency tests submit a stale snapshot while exercising the same
   * root-first ordering as the public delete path.
   *
   * @param identifier the topic identity observed by the caller
   * @param observedTopicPO the topic row and OCC version observed by the caller
   */
  void deleteTopicWithVersion(NameIdentifier identifier, TopicPO observedTopicPO) {
    SessionUtils.doMultipleWithCommit(
        // Check the root version before touching relationships. If this snapshot is stale,
        // throwing here stops the transaction before any dependent row can be deleted.
        () ->
            OccWriteSupport.deleteWithVersion(
                () ->
                    SessionUtils.getWithoutCommit(
                        TopicMetaMapper.class,
                        mapper ->
                            mapper.softDeleteTopicMetasByTopicId(
                                observedTopicPO.getTopicId(), observedTopicPO.getCurrentVersion())),
                () -> topicWriteFailure(identifier, observedTopicPO)),
        () -> deleteTopicDependents(observedTopicPO.getTopicId()));
  }

  private void deleteTopicDependents(Long topicId) {
    // The topic row has passed its version check. Every cleanup below uses the same transaction,
    // so a later failure also restores the root row and all earlier relationship changes.
    SessionUtils.doWithoutCommit(
        OwnerMetaMapper.class,
        mapper ->
            mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                topicId, MetadataObject.Type.TOPIC.name()));
    SessionUtils.doWithoutCommit(
        SecurableObjectMapper.class,
        mapper ->
            mapper.softDeleteObjectRelsByMetadataObject(topicId, MetadataObject.Type.TOPIC.name()));
    SessionUtils.doWithoutCommit(
        TagMetadataObjectRelMapper.class,
        mapper ->
            mapper.softDeleteTagMetadataObjectRelsByMetadataObject(
                topicId, MetadataObject.Type.TOPIC.name()));
    SessionUtils.doWithoutCommit(
        StatisticMetaMapper.class, mapper -> mapper.softDeleteStatisticsByEntityId(topicId));
    SessionUtils.doWithoutCommit(
        PolicyMetadataObjectRelMapper.class,
        mapper ->
            mapper.softDeletePolicyMetadataObjectRelsByMetadataObject(
                topicId, MetadataObject.Type.TOPIC.name()));
  }

  private RuntimeException topicWriteFailure(NameIdentifier identifier, TopicPO observedTopicPO) {
    // A zero-row CAS means either the same topic has a newer version, or the topic disappeared
    // from the name the caller used. The stable-ID lock waits for an in-flight writer to finish so
    // the result is classified from committed identity data.
    return OccWriteSupport.writeFailure(
        identifier,
        Entity.EntityType.TOPIC,
        () ->
            SessionUtils.getWithoutCommit(
                TopicMetaMapper.class,
                mapper -> mapper.selectTopicMetaByIdForUpdate(observedTopicPO.getTopicId())),
        null,
        current ->
            Objects.equals(current.getTopicName(), observedTopicPO.getTopicName())
                && Objects.equals(current.getSchemaId(), observedTopicPO.getSchemaId())
                && Objects.equals(current.getCatalogId(), observedTopicPO.getCatalogId())
                && Objects.equals(current.getMetalakeId(), observedTopicPO.getMetalakeId()));
  }
}
