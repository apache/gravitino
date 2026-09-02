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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
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
import org.apache.gravitino.storage.relational.po.ViewVersionInfoPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

/** The service class for view metadata. It provides the basic database operations for view. */
public class ViewMetaService {

  private static final ViewMetaService INSTANCE = new ViewMetaService();
  private BasePOStorageOps<ViewPO, ViewMetaMapper> ops;

  public static ViewMetaService getInstance() {
    return INSTANCE;
  }

  private ViewMetaService() {
    this.ops = new HierarchicalConversionPOStorageOps<>(new ViewPOStorageOps());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getViewIdBySchemaIdAndName")
  public Long getViewIdBySchemaIdAndName(Long schemaId, String viewName) {
    ViewPO viewPO =
        SessionUtils.getWithoutCommit(
            ViewMetaMapper.class, mapper -> ops.getPO(mapper, schemaId, viewName));

    if (viewPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.VIEW.name().toLowerCase(),
          viewName);
    }
    return viewPO.getViewId();
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listViewsByNamespace")
  public List<ViewEntity> listViewsByNamespace(Namespace namespace) {
    NamespaceUtil.checkView(namespace);
    List<ViewPO> viewPOs = listViewPOs(namespace);
    return viewPOs.stream().map(po -> fromViewPO(po, namespace)).collect(Collectors.toList());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getViewByIdentifier")
  public ViewEntity getViewByIdentifier(NameIdentifier identifier) {
    ViewPO viewPO = getViewPOByIdentifier(identifier);
    return fromViewPO(viewPO, identifier.namespace());
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "insertView")
  public void insertView(ViewEntity viewEntity, boolean overwrite) throws IOException {
    NameIdentifierUtil.checkView(viewEntity.nameIdentifier());

    ViewPO.ViewPOBuilder builder = ViewPO.builder();
    try {
      ViewPO po = initializeViewPO(viewEntity, builder);
      AtomicReference<ViewPO> persistedPO = new AtomicReference<>(po);

      SessionUtils.doMultipleWithCommit(
          // Hold the parent schema row until this transaction ends, so the view cannot be
          // written below a schema that is being dropped.
          () ->
              SchemaMetaService.getInstance()
                  .lockSchemaForEntityWrite(
                      viewEntity.nameIdentifier(),
                      po.getSchemaId(),
                      po.getCatalogId(),
                      po.getMetalakeId()),
          () ->
              SessionUtils.doWithoutCommit(
                  ViewMetaMapper.class,
                  mapper -> {
                    ops.insertPO(mapper, po, overwrite);
                    if (overwrite) {
                      ViewPO storedPO =
                          mapper.selectViewMetaBySchemaIdAndName(
                              po.getSchemaId(), po.getViewName());
                      Preconditions.checkState(
                          storedPO != null,
                          "The overwritten view %s in schema %s does not exist",
                          po.getViewName(),
                          po.getSchemaId());
                      persistedPO.set(viewPOWithPersistedIdentityAndVersions(po, storedPO));
                    }
                  }),
          () ->
              SessionUtils.doWithoutCommit(
                  ViewVersionInfoMapper.class,
                  mapper -> {
                    if (overwrite) {
                      mapper.insertViewVersionInfoOnDuplicateKeyUpdate(
                          persistedPO.get().getViewVersionInfoPO());
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
    ViewPO oldViewPO = getViewPOByIdentifier(ident);
    ViewEntity oldViewEntity = fromViewPO(oldViewPO, ident.namespace());
    ViewEntity newEntity = (ViewEntity) updater.apply((E) oldViewEntity);
    Preconditions.checkArgument(
        Objects.equals(oldViewEntity.id(), newEntity.id()),
        "The updated view entity id: %s should be same with the entity id before: %s",
        newEntity.id(),
        oldViewEntity.id());

    boolean isSchemaChanged = !newEntity.namespace().equals(oldViewEntity.namespace());
    Long newSchemaId =
        isSchemaChanged
            ? EntityIdService.getEntityId(
                NameIdentifier.of(newEntity.namespace().levels()), Entity.EntityType.SCHEMA)
            : oldViewPO.getSchemaId();

    try {
      ViewPO newViewPO = updateViewPO(oldViewPO, newEntity, newSchemaId);
      SessionUtils.doMultipleWithCommit(
          () -> {
            if (isSchemaChanged) {
              SchemaMetaService.getInstance()
                  .lockSchemaForEntityWrite(
                      newEntity.nameIdentifier(),
                      newSchemaId,
                      oldViewPO.getCatalogId(),
                      oldViewPO.getMetalakeId());
            }
          },
          () -> {
            // current_version is the sole OCC token. The root CAS is the transaction's decision
            // point and must run before the unguarded version-row insert below.
            int updated =
                SessionUtils.getWithoutCommit(
                    ViewMetaMapper.class, mapper -> ops.updatePO(mapper, newViewPO, oldViewPO));
            if (updated == 0) {
              throw viewWriteFailure(ident, oldViewPO);
            }
          },
          () ->
              SessionUtils.doWithoutCommit(
                  ViewVersionInfoMapper.class,
                  mapper -> mapper.insertViewVersionInfo(newViewPO.getViewVersionInfoPO())));
      return newEntity;
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.VIEW, newEntity.nameIdentifier().toString());
      throw re;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteViewByIdentifier")
  public boolean deleteView(NameIdentifier ident) {
    ViewPO viewPO = getViewPOByIdentifier(ident);

    deleteViewWithVersion(ident, viewPO);
    return true;
  }

  /**
   * Deletes the observed view and its dependent rows in one transaction.
   *
   * <p>Package-private access lets concurrency tests submit a deliberately stale snapshot while
   * exercising the same root-first ordering as the public delete path.
   */
  void deleteViewWithVersion(NameIdentifier identifier, ViewPO observedViewPO) {
    SessionUtils.doMultipleWithCommit(
        // Check the root version before touching relationships. A stale drop stops here.
        () ->
            OccWriteSupport.deleteWithVersion(
                () ->
                    SessionUtils.getWithoutCommit(
                        ViewMetaMapper.class,
                        mapper ->
                            mapper.softDeleteViewMetasByViewId(
                                observedViewPO.getViewId(), observedViewPO.getCurrentVersion())),
                () -> viewWriteFailure(identifier, observedViewPO)),
        () -> deleteViewDependents(observedViewPO.getViewId()));
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

  public BasePOStorageOps<ViewPO, ViewMetaMapper> ops() {
    return ops;
  }

  private ViewPO updateViewPO(ViewPO oldViewPO, ViewEntity newEntity, Long newSchemaId) {
    Long newVersion = oldViewPO.getLastVersion() + 1;
    ViewPO.ViewPOBuilder builder =
        ViewPO.builder()
            .withMetalakeId(oldViewPO.getMetalakeId())
            .withCatalogId(oldViewPO.getCatalogId())
            .withSchemaId(newSchemaId)
            .withCurrentVersion(newVersion)
            .withLastVersion(newVersion);
    return buildViewPO(newEntity, builder, newVersion.intValue());
  }

  ViewPO getViewPOByIdentifier(NameIdentifier identifier) {
    NameIdentifierUtil.checkView(identifier);
    ViewPO viewPO =
        SessionUtils.getWithoutCommit(
            ViewMetaMapper.class,
            mapper -> POStorageReadRouting.getPO(mapper, identifier, ops, Entity.EntityType.VIEW));
    if (viewPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.VIEW.name().toLowerCase(),
          identifier.name());
    }

    return viewPO;
  }

  private List<ViewPO> listViewPOs(Namespace namespace) {
    return SessionUtils.getWithoutCommit(
        ViewMetaMapper.class,
        mapper -> POStorageReadRouting.listPOs(mapper, namespace, ops, Entity.EntityType.VIEW));
  }

  private ViewPO viewPOWithPersistedIdentityAndVersions(ViewPO incomingPO, ViewPO persistedPO) {
    ViewVersionInfoPO incomingVersionPO = incomingPO.getViewVersionInfoPO();
    ViewVersionInfoPO persistedVersionPO =
        ViewVersionInfoPO.builder()
            .withMetalakeId(incomingVersionPO.metalakeId())
            .withCatalogId(incomingVersionPO.catalogId())
            .withSchemaId(incomingVersionPO.schemaId())
            .withViewId(persistedPO.getViewId())
            .withVersion(persistedPO.getCurrentVersion().intValue())
            .withViewComment(incomingVersionPO.viewComment())
            .withColumns(incomingVersionPO.columns())
            .withProperties(incomingVersionPO.properties())
            .withDefaultCatalog(incomingVersionPO.defaultCatalog())
            .withDefaultSchema(incomingVersionPO.defaultSchema())
            .withRepresentations(incomingVersionPO.representations())
            .withAuditInfo(incomingVersionPO.auditInfo())
            .withDeletedAt(incomingVersionPO.deletedAt())
            .build();
    return ViewPO.builder()
        .withViewId(persistedPO.getViewId())
        .withViewName(incomingPO.getViewName())
        .withMetalakeId(incomingPO.getMetalakeId())
        .withCatalogId(incomingPO.getCatalogId())
        .withSchemaId(incomingPO.getSchemaId())
        .withAuditInfo(incomingPO.getAuditInfo())
        .withCurrentVersion(persistedPO.getCurrentVersion())
        .withLastVersion(persistedPO.getLastVersion())
        .withDeletedAt(incomingPO.getDeletedAt())
        .withViewVersionInfoPO(persistedVersionPO)
        .build();
  }

  private void deleteViewDependents(Long viewId) {
    SessionUtils.doWithoutCommit(
        ViewVersionInfoMapper.class, mapper -> mapper.softDeleteViewVersionsByViewId(viewId));
    SessionUtils.doWithoutCommit(
        OwnerMetaMapper.class,
        mapper ->
            mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                viewId, MetadataObject.Type.VIEW.name()));
    SessionUtils.doWithoutCommit(
        SecurableObjectMapper.class,
        mapper ->
            mapper.softDeleteObjectRelsByMetadataObject(viewId, MetadataObject.Type.VIEW.name()));
    SessionUtils.doWithoutCommit(
        TagMetadataObjectRelMapper.class,
        mapper ->
            mapper.softDeleteTagMetadataObjectRelsByMetadataObject(
                viewId, MetadataObject.Type.VIEW.name()));
  }

  private RuntimeException viewWriteFailure(NameIdentifier identifier, ViewPO observedViewPO) {
    return OccWriteSupport.writeFailure(
        identifier,
        Entity.EntityType.VIEW,
        () ->
            SessionUtils.getWithoutCommit(
                ViewMetaMapper.class,
                mapper -> mapper.selectViewMetaByIdForUpdate(observedViewPO.getViewId())),
        null,
        current ->
            Objects.equals(current.getViewName(), observedViewPO.getViewName())
                && Objects.equals(current.getSchemaId(), observedViewPO.getSchemaId())
                && Objects.equals(current.getCatalogId(), observedViewPO.getCatalogId())
                && Objects.equals(current.getMetalakeId(), observedViewPO.getMetalakeId()));
  }
}
