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
import static org.apache.gravitino.storage.relational.po.ViewPO.buildViewPO;
import static org.apache.gravitino.storage.relational.po.ViewPO.fromViewPO;
import static org.apache.gravitino.storage.relational.po.ViewPO.initializeViewPO;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.mapper.ViewMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ViewVersionInfoMapper;
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

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listViewsByNamespace")
  public List<ViewEntity> listViewsByNamespace(Namespace namespace) {
    NamespaceUtil.checkView(namespace);
    List<ViewPO> viewPOs = listViewPOsByNamespace(namespace);
    return viewPOs.stream().map(po -> fromViewPO(po, namespace)).collect(Collectors.toList());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getViewByIdentifier")
  public ViewEntity getViewByIdentifier(NameIdentifier identifier) {
    NameIdentifierUtil.checkView(identifier);
    ViewPO viewPO = viewPOFetcher().apply(identifier);
    return fromViewPO(viewPO, identifier.namespace());
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "insertView")
  public void insertView(ViewEntity viewEntity, boolean overwrite) throws IOException {
    NameIdentifierUtil.checkView(viewEntity.nameIdentifier());

    ViewPO.Builder builder = ViewPO.builder();
    try {
      ViewPO po = initializeViewPO(viewEntity, builder);

      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  ViewMetaMapper.class,
                  mapper -> {
                    if (overwrite) {
                      mapper.insertViewMetaOnDuplicateKeyUpdate(po);
                    } else {
                      mapper.insertViewMeta(po);
                    }
                  }),
          () ->
              SessionUtils.doWithoutCommit(
                  ViewVersionInfoMapper.class,
                  mapper -> {
                    if (overwrite) {
                      mapper.insertViewVersionInfoOnDuplicateKeyUpdate(po.getViewVersionInfoPO());
                    } else {
                      mapper.insertViewVersionInfo(po.getViewVersionInfoPO());
                    }
                  }));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.VIEW, viewEntity.nameIdentifier().toString());
      throw re;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateViewByIdentifier")
  public <E extends Entity & HasIdentifier> ViewEntity updateView(
      NameIdentifier ident, Function<E, E> updater) throws IOException {
    NameIdentifierUtil.checkView(ident);

    ViewPO oldViewPO = viewPOFetcher().apply(ident);
    ViewEntity oldViewEntity = fromViewPO(oldViewPO, ident.namespace());
    ViewEntity newEntity = (ViewEntity) updater.apply((E) oldViewEntity);
    Preconditions.checkArgument(
        Objects.equals(oldViewEntity.id(), newEntity.id()),
        "The updated view entity id: %s should be same with the entity id before: %s",
        newEntity.id(),
        oldViewEntity.id());

    AtomicInteger updateResult = new AtomicInteger(0);
    try {
      ViewPO newViewPO = updateViewPO(oldViewPO, newEntity);
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  ViewVersionInfoMapper.class,
                  mapper -> mapper.insertViewVersionInfo(newViewPO.getViewVersionInfoPO())),
          () -> {
            updateResult.set(
                SessionUtils.getWithoutCommit(
                    ViewMetaMapper.class, mapper -> mapper.updateViewMeta(newViewPO, oldViewPO)));
            if (updateResult.get() == 0) {
              throw new RuntimeException("Failed to update the entity: " + ident);
            }
          });
      return newEntity;
    } catch (RuntimeException re) {
      if (updateResult.get() == 0) {
        throw new IOException("Failed to update the entity: " + ident);
      }
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.VIEW, newEntity.nameIdentifier().toString());
      throw re;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteViewByIdentifier")
  public boolean deleteView(NameIdentifier ident) {
    NameIdentifierUtil.checkView(ident);
    ViewPO viewPO = viewPOFetcher().apply(ident);
    return deleteView(viewPO.getViewId());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteViewMetasByLegacyTimeline")
  public int deleteViewMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    int versionDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            ViewVersionInfoMapper.class,
            mapper -> mapper.deleteViewVersionsByLegacyTimeline(legacyTimeline, limit));

    int metaDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            ViewMetaMapper.class,
            mapper -> mapper.deleteViewMetasByLegacyTimeline(legacyTimeline, limit));

    return versionDeletedCount + metaDeletedCount;
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
                ViewVersionInfoMapper.class,
                mapper -> mapper.softDeleteViewVersionsByViewId(viewId));
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

  private ViewPO updateViewPO(ViewPO oldViewPO, ViewEntity newEntity) {
    Long newVersion = oldViewPO.getLastVersion() + 1;
    ViewPO.Builder builder =
        ViewPO.builder()
            .withMetalakeId(oldViewPO.getMetalakeId())
            .withCatalogId(oldViewPO.getCatalogId())
            .withSchemaId(oldViewPO.getSchemaId())
            .withCurrentVersion(newVersion)
            .withLastVersion(newVersion);
    return buildViewPO(newEntity, builder, newVersion.intValue());
  }

  private List<ViewPO> listViewPOsByNamespace(Namespace namespace) {
    return viewListFetcher().apply(namespace);
  }

  private Function<Namespace, List<ViewPO>> viewListFetcher() {
    return GravitinoEnv.getInstance().cacheEnabled()
        ? this::listViewPOsBySchemaId
        : this::listViewPOsByFullQualifiedName;
  }

  private Function<NameIdentifier, ViewPO> viewPOFetcher() {
    return GravitinoEnv.getInstance().cacheEnabled()
        ? this::getViewPOBySchemaId
        : this::getViewPOByFullQualifiedName;
  }

  private List<ViewPO> listViewPOsBySchemaId(Namespace namespace) {
    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(namespace.levels()), Entity.EntityType.SCHEMA);
    return SessionUtils.getWithoutCommit(
        ViewMetaMapper.class, mapper -> mapper.listViewPOsBySchemaId(schemaId));
  }

  private List<ViewPO> listViewPOsByFullQualifiedName(Namespace namespace) {
    String[] namespaceLevels = namespace.levels();
    List<ViewPO> viewPOs =
        SessionUtils.getWithoutCommit(
            ViewMetaMapper.class,
            mapper ->
                mapper.listViewPOsByFullQualifiedName(
                    namespaceLevels[0], namespaceLevels[1], namespaceLevels[2]));
    if (viewPOs.isEmpty() || viewPOs.get(0).getSchemaId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          EntityType.SCHEMA.name().toLowerCase(),
          namespaceLevels[2]);
    }
    return viewPOs.stream().filter(po -> po.getViewId() != null).collect(Collectors.toList());
  }

  private ViewPO getViewPOBySchemaId(NameIdentifier identifier) {
    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(identifier.namespace().levels()), Entity.EntityType.SCHEMA);
    ViewPO viewPO =
        SessionUtils.getWithoutCommit(
            ViewMetaMapper.class,
            mapper -> mapper.selectViewMetaBySchemaIdAndName(schemaId, identifier.name()));

    if (viewPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.VIEW.name().toLowerCase(),
          identifier.name());
    }
    return viewPO;
  }

  private ViewPO getViewPOByFullQualifiedName(NameIdentifier identifier) {
    String[] namespaceLevels = identifier.namespace().levels();
    ViewPO viewPO =
        SessionUtils.getWithoutCommit(
            ViewMetaMapper.class,
            mapper ->
                mapper.selectViewByFullQualifiedName(
                    namespaceLevels[0], namespaceLevels[1], namespaceLevels[2], identifier.name()));

    if (viewPO == null || viewPO.getSchemaId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          EntityType.SCHEMA.name().toLowerCase(),
          namespaceLevels[2]);
    }

    if (viewPO.getViewId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          EntityType.VIEW.name().toLowerCase(),
          identifier.name());
    }

    return viewPO;
  }
}
