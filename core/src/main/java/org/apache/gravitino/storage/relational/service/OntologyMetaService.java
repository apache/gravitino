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
import static org.apache.gravitino.storage.relational.po.OntologyPO.fromOntologyPO;
import static org.apache.gravitino.storage.relational.po.OntologyPO.initializeOntologyPO;
import static org.apache.gravitino.storage.relational.po.OntologyPO.rebuildOntologyPO;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.OntologyEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.ontology.Ontology;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OntologyMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OntologyVersionInfoMapper;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.OntologyPO;
import org.apache.gravitino.storage.relational.po.OntologyVersionInfoPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides relational persistence operations for metalake-level ontology metadata.
 *
 * <p>The service keeps two tables per ontology: an identity row that owns the name, the status and
 * the version pointers, and one immutable snapshot row per version that owns the comment,
 * properties and definition content. Reads always resolve through {@code
 * ontology_meta.current_version}, so a snapshot is never loaded by an unverified version number.
 *
 * <p>Two write paths exist on purpose:
 *
 * <ul>
 *   <li>Any content change (comment, properties, definition sections or the name) allocates a new
 *       version and moves {@code current_version} to it. {@link #updateOntology} takes that path.
 *   <li>A status change is a lifecycle flag flip on the identity row. It must not allocate a
 *       version, because no content changed and a published version number must stay stable across
 *       a draft/published toggle. {@link #updateOntologyStatus} takes that path, and {@link
 *       #updateOntology} delegates to it when the update leaves every content field unchanged.
 * </ul>
 *
 * <p>Deletes are soft: the identity and its snapshots keep their rows with a non-zero {@code
 * deleted_at}, exactly like the semantic model.
 */
public class OntologyMetaService {

  private static final Logger LOG = LoggerFactory.getLogger(OntologyMetaService.class);
  private static final OntologyMetaService INSTANCE = new OntologyMetaService();

  private final BasePOStorageOps<OntologyPO, OntologyMetaMapper> ops;

  /**
   * Returns the process-wide ontology metadata service.
   *
   * @return The singleton service.
   */
  public static OntologyMetaService getInstance() {
    return INSTANCE;
  }

  private OntologyMetaService() {
    this.ops = new OntologyPOStorageOps();
  }

  /**
   * Returns the persistent-object operations this service delegates reads and writes to.
   *
   * @return The storage operations.
   */
  public BasePOStorageOps<OntologyPO, OntologyMetaMapper> ops() {
    return ops;
  }

  /**
   * Returns the ID of an active ontology in a metalake.
   *
   * @param metalakeId The metalake ID.
   * @param ontologyName The ontology name.
   * @return The ontology ID.
   * @throws NoSuchEntityException If the ontology does not exist.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getOntologyIdByMetalakeIdAndName")
  public Long getOntologyIdByMetalakeIdAndName(long metalakeId, String ontologyName) {
    Long ontologyId =
        SessionUtils.getWithoutCommit(
            OntologyMetaMapper.class,
            mapper -> mapper.selectOntologyIdByMetalakeIdAndName(metalakeId, ontologyName));
    if (ontologyId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, "ontology", ontologyName);
    }
    return ontologyId;
  }

  /**
   * Lists the ontologies of a metalake.
   *
   * @param namespace The one-level namespace holding the metalake name.
   * @return The ontologies, ordered as the store returns them.
   * @throws NoSuchEntityException If the metalake does not exist.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listOntologiesByNamespace")
  public List<OntologyEntity> listOntologiesByNamespace(Namespace namespace) {
    checkNamespace(namespace);
    List<OntologyPO> ontologyPOs =
        SessionUtils.getWithoutCommit(
            OntologyMetaMapper.class,
            mapper ->
                POStorageReadRouting.listPOs(mapper, namespace, ops, Entity.EntityType.ONTOLOGY));
    return ontologyPOs.stream()
        .map(ontologyPO -> fromOntologyPO(ontologyPO, namespace))
        .collect(Collectors.toList());
  }

  /**
   * Loads one ontology by identifier.
   *
   * @param identifier The ontology identifier, a metalake namespace plus the ontology name.
   * @return The ontology.
   * @throws NoSuchEntityException If the metalake or the ontology does not exist.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getOntologyByIdentifier")
  public OntologyEntity getOntologyByIdentifier(NameIdentifier identifier) {
    checkIdentifier(identifier);
    return fromOntologyPO(getOntologyPOByIdentifier(identifier), identifier.namespace());
  }

  /**
   * Creates an ontology, or replaces the active ontology the entity resolves to.
   *
   * <p>A create allocates version 1. An overwrite allocates the next version of the row it
   * replaced, so the persisted ID and the version sequence stay stable.
   *
   * @param ontologyEntity The ontology to store.
   * @param overwrite Whether an existing ontology may be replaced.
   * @throws IOException If the metalake cannot be read.
   * @throws EntityAlreadyExistsException If the ontology exists and {@code overwrite} is false.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertOntology")
  public void insertOntology(OntologyEntity ontologyEntity, boolean overwrite) throws IOException {
    Namespace namespace = ontologyEntity.namespace();
    checkNamespace(namespace);
    try {
      MetalakePO metalakePO =
          MetalakeMetaService.getInstance().requireMetalakeByName(namespace.level(0));
      OntologyPO ontologyPO =
          initializeOntologyPO(ontologyEntity, metalakePO.getMetalakeId(), OntologyPO.builder());

      SessionUtils.doMultipleWithCommit(
          () -> lockMetalakeForOntologyCreate(metalakePO),
          () -> insertOntologyWithoutCommit(ontologyEntity, ontologyPO, overwrite));
    } catch (RuntimeException e) {
      try {
        ExceptionUtils.checkSQLException(
            e, Entity.EntityType.ONTOLOGY, ontologyEntity.nameIdentifier().toString());
      } catch (EntityAlreadyExistsException duplicate) {
        if (overwrite) {
          // A missing-row locking read does not fence a concurrent insert at READ_COMMITTED.
          // Propagate the conflict so the whole transaction is rolled back before retrying.
          throw ExceptionUtils.concurrentModification(
              Entity.EntityType.ONTOLOGY, ontologyEntity.nameIdentifier());
        }
        throw duplicate;
      }
      throw e;
    }
  }

  /**
   * Applies an update produced by {@code updater}.
   *
   * <p>When the update changes any content field, a new version is allocated and becomes the
   * current version. When the only difference is the status, the identity row is updated in place
   * and no version is allocated, because a status change is a lifecycle flag flip rather than new
   * content.
   *
   * @param identifier The ontology identifier.
   * @param updater The update function.
   * @param <E> The entity type the updater consumes.
   * @return The updated ontology.
   * @throws IOException If the read or write fails.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateOntology")
  public <E extends Entity & HasIdentifier> OntologyEntity updateOntology(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    checkIdentifier(identifier);

    OntologyPO oldPO = getOntologyPOByIdentifier(identifier);
    OntologyEntity oldEntity = fromOntologyPO(oldPO, identifier.namespace());
    OntologyEntity newEntity = (OntologyEntity) updater.apply((E) oldEntity);
    Preconditions.checkArgument(newEntity != null, "The updated ontology entity must not be null");
    Preconditions.checkArgument(
        Objects.equals(oldEntity.id(), newEntity.id()),
        "The updated ontology entity id: %s must have the same id as the old entity id %s",
        newEntity.id(),
        oldEntity.id());
    Preconditions.checkArgument(
        Objects.equals(oldEntity.namespace(), newEntity.namespace()),
        "The updated ontology entity namespace: %s must have the same namespace as the old"
            + " namespace %s",
        newEntity.namespace(),
        oldEntity.namespace());

    if (onlyStatusChanged(oldEntity, newEntity)) {
      return updateOntologyStatus(identifier, oldPO, newEntity.status());
    }

    OntologyPO newPO = ontologyPOWithNewVersion(oldPO, newEntity);
    try {
      SessionUtils.doMultipleWithCommit(
          () -> updateOntologyIdentityWithVersion(identifier, oldPO, newPO),
          () ->
              SessionUtils.doWithoutCommit(
                  OntologyVersionInfoMapper.class,
                  mapper -> mapper.insertOntologyVersionInfo(newPO.getOntologyVersionInfoPO())));
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.ONTOLOGY, identifier.toString());
      throw e;
    }

    LOG.debug("Updated ontology {} to version {}", identifier.name(), newPO.getCurrentVersion());
    return newEntity;
  }

  /**
   * Flips the status of an ontology without allocating a version.
   *
   * <p>The identity row keeps {@code current_version} and {@code last_version}, so every existing
   * snapshot stays addressable by the version number it was published under.
   *
   * @param identifier The ontology identifier.
   * @param status The new status.
   * @return The updated ontology.
   * @throws IOException If the read or write fails.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateOntologyStatus")
  public OntologyEntity updateOntologyStatus(NameIdentifier identifier, Ontology.Status status)
      throws IOException {
    checkIdentifier(identifier);
    Preconditions.checkArgument(status != null, "The ontology status must not be null");
    return updateOntologyStatus(identifier, getOntologyPOByIdentifier(identifier), status);
  }

  /**
   * Soft-deletes an ontology and its snapshots.
   *
   * @param identifier The ontology identifier.
   * @return {@code true} if an active ontology was deleted, {@code false} if there was none.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteOntology")
  public boolean deleteOntology(NameIdentifier identifier) {
    checkIdentifier(identifier);

    OntologyPO ontologyPO;
    try {
      ontologyPO = getOntologyPOByIdentifier(identifier);
    } catch (NoSuchEntityException e) {
      return false;
    }
    return deleteOntologyWithVersion(identifier, ontologyPO);
  }

  /**
   * Permanently deletes soft-deleted ontology identities older than a timeline.
   *
   * @param legacyTimeline The timeline in milliseconds.
   * @param limit The maximum number of rows to delete.
   * @return The number of deleted identity rows.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteOntologyMetasByLegacyTimeline")
  public int deleteOntologyMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    // Snapshots go first so a partially collected ontology never keeps a version row whose
    // identity has already been purged.
    int deletedVersions =
        SessionUtils.doWithCommitAndFetchResult(
            OntologyVersionInfoMapper.class,
            mapper -> mapper.deleteOntologyVersionsByLegacyTimeline(legacyTimeline, limit));
    int deletedMetas =
        SessionUtils.doWithCommitAndFetchResult(
            OntologyMetaMapper.class,
            mapper -> mapper.deleteOntologyMetasByLegacyTimeline(legacyTimeline, limit));
    LOG.info(
        "Deleted {} ontology identity rows and {} ontology version rows older than {}",
        deletedMetas,
        deletedVersions,
        legacyTimeline);
    return deletedMetas + deletedVersions;
  }

  /**
   * Soft-deletes ontology snapshots that fell behind the version retention count.
   *
   * @param versionRetentionCount The number of versions to keep per ontology.
   * @param limit The maximum number of rows to delete per ontology.
   * @return The number of soft-deleted snapshot rows.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteOntologyVersionsByRetentionCount")
  public int deleteOntologyVersionsByRetentionCount(Long versionRetentionCount, int limit) {
    List<OntologyVersionInfoPO> ontologyPOs =
        SessionUtils.getWithoutCommit(
            OntologyVersionInfoMapper.class,
            mapper -> mapper.selectOntologyVersionsByRetentionCount(versionRetentionCount));

    int totalDeleted = 0;
    for (OntologyVersionInfoPO ontologyVersionPO : ontologyPOs) {
      long versionRetentionLine = ontologyVersionPO.version() - versionRetentionCount;
      int deleted =
          SessionUtils.doWithCommitAndFetchResult(
              OntologyVersionInfoMapper.class,
              mapper ->
                  mapper.softDeleteOntologyVersionsByRetentionLine(
                      ontologyVersionPO.ontologyId(), versionRetentionLine, limit));
      totalDeleted += deleted;
      LOG.info(
          "Soft deleted {} ontology versions through retention line {} of ontology <{}, {}>",
          deleted,
          versionRetentionLine,
          ontologyVersionPO.ontologyId(),
          ontologyVersionPO.version());
    }
    return totalDeleted;
  }

  /**
   * Loads the persistent object of an active ontology.
   *
   * @param identifier The ontology identifier.
   * @return The persistent object with its current snapshot.
   * @throws NoSuchEntityException If the metalake or the ontology does not exist.
   */
  OntologyPO getOntologyPOByIdentifier(NameIdentifier identifier) {
    OntologyPO ontologyPO =
        SessionUtils.getWithoutCommit(
            OntologyMetaMapper.class,
            mapper ->
                POStorageReadRouting.getPO(mapper, identifier, ops, Entity.EntityType.ONTOLOGY));
    if (ontologyPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, "ontology", identifier.name());
    }
    return ontologyPO;
  }

  /** Soft-deletes an ontology identity and its snapshots, for tests and for deletion cascades. */
  boolean deleteOntologyWithVersion(NameIdentifier identifier, OntologyPO observedOntologyPO) {
    // A delete that lost a race against another writer must stay idempotent, so the lost race
    // reports "nothing to delete" instead of failing the caller.
    try {
      SessionUtils.doMultipleWithCommit(
          () ->
              OccWriteSupport.deleteWithVersion(
                  () ->
                      SessionUtils.getWithoutCommit(
                          OntologyMetaMapper.class,
                          mapper ->
                              mapper.softDeleteOntologyMetasByOntologyId(
                                  observedOntologyPO.getOntologyId(),
                                  observedOntologyPO.getCurrentVersion())),
                  () -> ontologyWriteFailure(identifier, observedOntologyPO)),
          () ->
              SessionUtils.doWithoutCommit(
                  OntologyVersionInfoMapper.class,
                  mapper ->
                      mapper.softDeleteOntologyVersionsByOntologyId(
                          observedOntologyPO.getOntologyId())));
    } catch (NoSuchEntityException e) {
      LOG.debug("Ontology {} was already deleted or replaced", identifier);
      return false;
    }
    return true;
  }

  private OntologyEntity updateOntologyStatus(
      NameIdentifier identifier, OntologyPO observedOntologyPO, Ontology.Status status)
      throws IOException {
    OntologyPO newPO = ontologyPOWithStatus(observedOntologyPO, status);
    try {
      Integer updated =
          SessionUtils.doWithCommitAndFetchResult(
              OntologyMetaMapper.class,
              mapper -> mapper.updateOntologyMetaStatus(newPO, observedOntologyPO));
      if (updated == null || updated == 0) {
        throw ontologyWriteFailure(identifier, observedOntologyPO);
      }
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.ONTOLOGY, identifier.toString());
      throw e;
    }

    LOG.debug("Flipped ontology {} status to {}", identifier.name(), status);
    return fromOntologyPO(getOntologyPOByIdentifier(identifier), identifier.namespace());
  }

  private void insertOntologyWithoutCommit(
      OntologyEntity ontologyEntity, OntologyPO initializedOntologyPO, boolean overwritten) {
    if (!overwritten) {
      insertNewOntologyWithoutCommit(initializedOntologyPO);
      return;
    }

    OntologyPO existingOntologyPO = findAndLockOntologyForOverwrite(initializedOntologyPO);
    if (existingOntologyPO == null) {
      insertNewOntologyWithoutCommit(initializedOntologyPO);
      return;
    }

    NameIdentifier identifier =
        NameIdentifier.of(ontologyEntity.namespace(), existingOntologyPO.getOntologyName());
    OntologyPO replacementOntologyPO = ontologyPOWithNewVersion(existingOntologyPO, ontologyEntity);
    updateOntologyIdentityWithVersion(identifier, existingOntologyPO, replacementOntologyPO);
    SessionUtils.doWithoutCommit(
        OntologyVersionInfoMapper.class,
        mapper ->
            mapper.insertOntologyVersionInfo(replacementOntologyPO.getOntologyVersionInfoPO()));
  }

  private void insertNewOntologyWithoutCommit(OntologyPO ontologyPO) {
    SessionUtils.doWithoutCommit(
        OntologyMetaMapper.class, mapper -> mapper.insertOntologyMeta(ontologyPO));
    SessionUtils.doWithoutCommit(
        OntologyVersionInfoMapper.class,
        mapper -> mapper.insertOntologyVersionInfo(ontologyPO.getOntologyVersionInfoPO()));
  }

  private OntologyPO findAndLockOntologyForOverwrite(OntologyPO initializedOntologyPO) {
    return OccWriteSupport.findAndLockForOverwrite(
        () ->
            SessionUtils.getWithoutCommit(
                OntologyMetaMapper.class,
                mapper ->
                    mapper.selectOntologyMetaByMetalakeIdAndNameForUpdate(
                        initializedOntologyPO.getMetalakeId(),
                        initializedOntologyPO.getOntologyName())),
        () ->
            SessionUtils.getWithoutCommit(
                OntologyMetaMapper.class,
                mapper ->
                    mapper.selectOntologyMetaByIdForUpdate(initializedOntologyPO.getOntologyId())),
        current -> Objects.equals(current.getMetalakeId(), initializedOntologyPO.getMetalakeId()));
  }

  private void updateOntologyIdentityWithVersion(
      NameIdentifier identifier, OntologyPO oldPO, OntologyPO newPO) {
    Integer updated =
        SessionUtils.getWithoutCommit(
            OntologyMetaMapper.class, mapper -> ops.updatePO(mapper, newPO, oldPO));
    if (updated == null || updated == 0) {
      throw ontologyWriteFailure(identifier, oldPO);
    }
  }

  private OntologyPO ontologyPOWithNewVersion(
      OntologyPO observedOntologyPO, OntologyEntity ontologyEntity) {
    int nextVersion =
        Math.max(observedOntologyPO.getCurrentVersion(), observedOntologyPO.getLastVersion()) + 1;
    return rebuildOntologyPO(observedOntologyPO, ontologyEntity, nextVersion);
  }

  private OntologyPO ontologyPOWithStatus(OntologyPO observedOntologyPO, Ontology.Status status) {
    return OntologyPO.builder()
        .withOntologyId(observedOntologyPO.getOntologyId())
        .withOntologyName(observedOntologyPO.getOntologyName())
        .withMetalakeId(observedOntologyPO.getMetalakeId())
        .withStatus(status.name())
        .withCurrentVersion(observedOntologyPO.getCurrentVersion())
        .withLastVersion(observedOntologyPO.getLastVersion())
        .withAuditInfo(observedOntologyPO.getAuditInfo())
        .withOntologyVersionInfoPO(observedOntologyPO.getOntologyVersionInfoPO())
        .withDeletedAt(observedOntologyPO.getDeletedAt())
        .build();
  }

  private static boolean onlyStatusChanged(OntologyEntity oldEntity, OntologyEntity newEntity) {
    return oldEntity.status() != newEntity.status()
        && Objects.equals(oldEntity.name(), newEntity.name())
        && Objects.equals(oldEntity.comment(), newEntity.comment())
        && Objects.equals(oldEntity.definition(), newEntity.definition())
        && Objects.equals(oldEntity.properties(), newEntity.properties());
  }

  private RuntimeException ontologyWriteFailure(
      NameIdentifier identifier, OntologyPO observedOntologyPO) {
    return OccWriteSupport.writeFailure(
        identifier,
        Entity.EntityType.ONTOLOGY,
        () ->
            SessionUtils.getWithoutCommit(
                OntologyMetaMapper.class,
                mapper ->
                    mapper.selectOntologyMetaByIdForUpdate(observedOntologyPO.getOntologyId())),
        null,
        current ->
            Objects.equals(current.getOntologyName(), observedOntologyPO.getOntologyName())
                && Objects.equals(current.getMetalakeId(), observedOntologyPO.getMetalakeId()));
  }

  private void lockMetalakeForOntologyCreate(MetalakePO observedMetalakePO) {
    OccWriteSupport.lockParentForChildWrite(
        observedMetalakePO.getMetalakeName(),
        Entity.EntityType.METALAKE,
        () ->
            SessionUtils.getWithoutCommit(
                MetalakeMetaMapper.class,
                mapper ->
                    mapper.selectMetalakeMetaByIdForShare(observedMetalakePO.getMetalakeId())),
        null,
        current -> Objects.equals(current.getMetalakeName(), observedMetalakePO.getMetalakeName()));
  }

  private static void checkNamespace(Namespace namespace) {
    Namespace.check(
        namespace != null && namespace.length() == 1,
        "Ontology namespace must be non-null and have 1 level, the input namespace is %s",
        namespace);
  }

  private static void checkIdentifier(NameIdentifier identifier) {
    NameIdentifier.check(identifier != null, "Ontology identifier must not be null");
    checkNamespace(identifier.namespace());
  }
}
