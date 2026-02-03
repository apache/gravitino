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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.mapper.ViewMetaMapper;
import org.apache.gravitino.storage.relational.po.ViewPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

/** The service class for view metadata. It provides the basic database operations for view. */
public class ViewMetaService {
  private static final ViewMetaService INSTANCE = new ViewMetaService();

  public static ViewMetaService getInstance() {
    return INSTANCE;
  }

  private ViewMetaService() {}

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getViewIdBySchemaIdAndName")
  public Long getViewIdBySchemaIdAndName(Long schemaId, String viewName) {
    Long viewId =
        SessionUtils.getWithoutCommit(
            ViewMetaMapper.class,
            mapper -> mapper.selectViewIdBySchemaIdAndName(schemaId, viewName));

    if (viewId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.VIEW.name().toLowerCase(),
          viewName);
    }
    return viewId;
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "insertView")
  public void insertView(ViewPO viewPO, boolean overwrite) throws IOException {
    try {
      SessionUtils.doWithCommit(
          ViewMetaMapper.class,
          mapper -> {
            if (overwrite) {
              mapper.insertViewMetaOnDuplicateKeyUpdate(viewPO);
            } else {
              mapper.insertViewMeta(viewPO);
            }
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(re, Entity.EntityType.VIEW, viewPO.getViewName());
      throw re;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteViewMetasByLegacyTimeline")
  public int deleteViewMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        ViewMetaMapper.class,
        mapper -> mapper.deleteViewMetasByLegacyTimeline(legacyTimeline, limit));
  }

  /**
   * List views as GenericEntity by namespace.
   *
   * @param namespace The namespace to list views from.
   * @return A list of GenericEntity representing views.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listViewsByNamespace")
  public List<GenericEntity> listViewsByNamespace(Namespace namespace) {
    NamespaceUtil.checkView(namespace);
    List<ViewPO> viewPOs = listViewPOsByNamespace(namespace);
    return viewPOs.stream()
        .map(
            viewPO ->
                GenericEntity.builder()
                    .withId(viewPO.getViewId())
                    .withName(viewPO.getViewName())
                    .withEntityType(Entity.EntityType.VIEW)
                    .build())
        .collect(Collectors.toList());
  }

  /**
   * Get a view as GenericEntity by identifier.
   *
   * @param identifier The identifier of the view.
   * @return The GenericEntity representing the view.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getViewByIdentifier")
  public GenericEntity getViewByIdentifier(NameIdentifier identifier) {
    NameIdentifierUtil.checkView(identifier);

    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifierUtil.getSchemaIdentifier(identifier), Entity.EntityType.SCHEMA);
    ViewPO viewPO = getViewPOBySchemaIdAndName(schemaId, identifier.name());

    return GenericEntity.builder()
        .withId(viewPO.getViewId())
        .withName(viewPO.getViewName())
        .withEntityType(Entity.EntityType.VIEW)
        .build();
  }

  /**
   * Insert a view from GenericEntity.
   *
   * @param entity The GenericEntity representing the view.
   * @param overwrite Whether to overwrite an existing view.
   * @throws IOException If an I/O error occurs.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertViewFromEntity")
  public void insertView(GenericEntity entity, boolean overwrite) throws IOException {
    Namespace namespace = entity.namespace();
    NamespaceUtil.checkView(namespace);

    ViewPO.Builder builder = createViewPOBuilder(namespace);
    ViewPO viewPO =
        builder
            .withViewId(entity.id())
            .withViewName(entity.name())
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .build();

    insertView(viewPO, overwrite);
  }

  /**
   * Update a view.
   *
   * @param ident The identifier of the view.
   * @param updater The function to update the view entity.
   * @return The updated GenericEntity.
   * @throws IOException If an I/O error occurs.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateViewByIdentifier")
  public <E extends HasIdentifier> GenericEntity updateView(
      NameIdentifier ident, Function<E, E> updater) throws IOException {
    NameIdentifierUtil.checkView(ident);

    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifierUtil.getSchemaIdentifier(ident), Entity.EntityType.SCHEMA);
    ViewPO oldViewPO = getViewPOBySchemaIdAndName(schemaId, ident.name());

    GenericEntity oldEntity =
        GenericEntity.builder()
            .withId(oldViewPO.getViewId())
            .withName(oldViewPO.getViewName())
            .withEntityType(Entity.EntityType.VIEW)
            .build();

    GenericEntity newEntity = (GenericEntity) updater.apply((E) oldEntity);

    ViewPO newViewPO =
        ViewPO.builder()
            .withViewId(oldViewPO.getViewId())
            .withViewName(newEntity.name())
            .withMetalakeId(oldViewPO.getMetalakeId())
            .withCatalogId(oldViewPO.getCatalogId())
            .withSchemaId(oldViewPO.getSchemaId())
            .withDeletedAt(oldViewPO.getDeletedAt())
            .withLastVersion(oldViewPO.getLastVersion())
            .withCurrentVersion(oldViewPO.getCurrentVersion())
            .build();

    updateView(oldViewPO, newViewPO);

    return newEntity;
  }

  /**
   * Delete a view by identifier.
   *
   * @param ident The identifier of the view.
   * @return true if the view was deleted, false otherwise.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteViewByIdentifier")
  public boolean deleteView(NameIdentifier ident) {
    NameIdentifierUtil.checkView(ident);

    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifierUtil.getSchemaIdentifier(ident), Entity.EntityType.SCHEMA);
    Long viewId = getViewIdBySchemaIdAndName(schemaId, ident.name());

    return deleteView(viewId);
  }

  private ViewPO getViewPOBySchemaIdAndName(Long schemaId, String viewName) {
    ViewPO viewPO =
        SessionUtils.getWithoutCommit(
            ViewMetaMapper.class,
            mapper -> mapper.selectViewMetaBySchemaIdAndName(schemaId, viewName));

    if (viewPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.VIEW.name().toLowerCase(),
          viewName);
    }
    return viewPO;
  }

  private boolean deleteView(Long viewId) {
    AtomicInteger deleteResult = new AtomicInteger(0);
    SessionUtils.doMultipleWithCommit(
        () ->
            deleteResult.set(
                SessionUtils.getWithoutCommit(
                    ViewMetaMapper.class, mapper -> mapper.softDeleteViewMetasByViewId(viewId))),
        () -> {
          if (deleteResult.get() > 0) {
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class,
                mapper ->
                    mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                        viewId, MetadataObject.Type.VIEW.name()));
            SessionUtils.doWithoutCommit(
                SecurableObjectMapper.class,
                mapper ->
                    mapper.softDeleteObjectRelsByMetadataObject(
                        viewId, MetadataObject.Type.VIEW.name()));
            SessionUtils.doWithoutCommit(
                TagMetadataObjectRelMapper.class,
                mapper ->
                    mapper.softDeleteTagMetadataObjectRelsByMetadataObject(
                        viewId, MetadataObject.Type.VIEW.name()));
          }
        });
    return deleteResult.get() > 0;
  }

  private void updateView(ViewPO oldViewPO, ViewPO newViewPO) throws IOException {
    try {
      Integer updateResult =
          SessionUtils.doWithCommitAndFetchResult(
              ViewMetaMapper.class, mapper -> mapper.updateViewMeta(newViewPO, oldViewPO));
      if (updateResult == 0) {
        throw new IOException("Failed to update the view: " + oldViewPO.getViewName());
      }
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(re, Entity.EntityType.VIEW, newViewPO.getViewName());
      throw re;
    }
  }

  private void fillViewPOBuilderParentEntityId(ViewPO.Builder builder, Namespace namespace) {
    NamespaceUtil.checkView(namespace);
    NamespacedEntityId namespacedEntityId =
        EntityIdService.getEntityIds(
            NameIdentifier.of(namespace.levels()), Entity.EntityType.SCHEMA);
    builder.withMetalakeId(namespacedEntityId.namespaceIds()[0]);
    builder.withCatalogId(namespacedEntityId.namespaceIds()[1]);
    builder.withSchemaId(namespacedEntityId.entityId());
  }

  private ViewPO.Builder createViewPOBuilder(Namespace namespace) {
    ViewPO.Builder builder = ViewPO.builder();
    fillViewPOBuilderParentEntityId(builder, namespace);
    return builder;
  }

  private List<ViewPO> listViewPOsByNamespace(Namespace namespace) {
    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(namespace.levels()), Entity.EntityType.SCHEMA);
    return SessionUtils.getWithoutCommit(
        ViewMetaMapper.class, mapper -> mapper.listViewPOsBySchemaId(schemaId));
  }
}
