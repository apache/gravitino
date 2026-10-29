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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.DomainMetaMapper;
import org.apache.gravitino.storage.relational.mapper.DomainRelMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper;
import org.apache.gravitino.storage.relational.po.DomainPO;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** The service that stores domains in the {@code domain_meta} table. */
public class DomainMetaService {

  private static final DomainMetaService INSTANCE = new DomainMetaService();

  private DomainMetaService() {}

  /**
   * Returns the singleton service instance.
   *
   * @return The domain meta service.
   */
  public static DomainMetaService getInstance() {
    return INSTANCE;
  }

  /**
   * Lists the domains of a metalake.
   *
   * @param ns The domain namespace of the metalake.
   * @return The domains of the metalake.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listDomainsByNamespace")
  public List<DomainEntity> listDomainsByNamespace(Namespace ns) {
    String metalakeName = ns.level(0);
    List<DomainPO> domainPOs =
        SessionUtils.getWithoutCommit(
            DomainMetaMapper.class, mapper -> mapper.listDomainPOsByMetalake(metalakeName));
    return fromDomainPOsWithParents(domainPOs, ns);
  }

  /**
   * Gets a domain by its identifier.
   *
   * @param ident The domain identifier.
   * @return The domain.
   * @throws NoSuchEntityException If the domain does not exist.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getDomainByIdentifier")
  public DomainEntity getDomainByIdentifier(NameIdentifier ident) {
    String metalakeName = ident.namespace().level(0);
    DomainPO domainPO = getDomainPOByMetalakeAndName(metalakeName, ident.name());
    return fromDomainPOWithParent(domainPO, ident.namespace());
  }

  /**
   * Gets the domains with the given identifiers. Identifiers of domains that do not exist are
   * skipped.
   *
   * @param identifiers The domain identifiers, all in the same metalake.
   * @return The domains that exist.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchGetDomainByIdentifier")
  public List<DomainEntity> batchGetDomainByIdentifier(List<NameIdentifier> identifiers) {
    if (identifiers.isEmpty()) {
      return Collections.emptyList();
    }
    NameIdentifier firstIdent = identifiers.get(0);
    String metalakeName = firstIdent.namespace().level(0);
    List<String> domainNames =
        identifiers.stream().map(NameIdentifier::name).collect(Collectors.toList());

    List<DomainPO> domainPOs =
        SessionUtils.getWithoutCommit(
            DomainMetaMapper.class,
            mapper -> mapper.listDomainPOsByMetalakeAndDomainNames(metalakeName, domainNames));
    return fromDomainPOsWithParents(domainPOs, firstIdent.namespace());
  }

  /**
   * Inserts a domain.
   *
   * @param domainEntity The domain to insert.
   * @param overwritten Whether to replace an active domain with the same name or ID.
   * @throws IOException If the domain cannot be stored.
   * @throws EntityAlreadyExistsException If a domain with the same name exists and {@code
   *     overwritten} is false.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertDomain")
  public void insertDomain(DomainEntity domainEntity, boolean overwritten) throws IOException {
    Namespace ns = domainEntity.namespace();
    String metalakeName = ns.level(0);

    try {
      MetalakePO metalakePO =
          SessionUtils.getWithoutCommit(
              MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(metalakeName));
      if (metalakePO == null) {
        throw new NoSuchEntityException(
            NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
            Entity.EntityType.METALAKE.name().toLowerCase(),
            metalakeName);
      }

      DomainPO domainPO = DomainPO.initializeDomainPO(domainEntity, metalakePO.getMetalakeId());

      SessionUtils.doMultipleWithCommit(
          () ->
              lockMetalakeForDomainWrite(metalakePO.getMetalakeName(), metalakePO.getMetalakeId()),
          () -> insertDomainWithoutCommit(domainEntity, domainPO, overwritten));
    } catch (RuntimeException e) {
      try {
        ExceptionUtils.checkSQLException(
            e, Entity.EntityType.DOMAIN, domainEntity.nameIdentifier().toString());
      } catch (EntityAlreadyExistsException duplicate) {
        if (overwritten) {
          // A missing-row locking read does not fence a concurrent insert at READ_COMMITTED.
          // Propagate the conflict so the whole transaction is rolled back before retrying.
          throw ExceptionUtils.concurrentModification(
              Entity.EntityType.DOMAIN, domainEntity.nameIdentifier());
        }
        throw duplicate;
      }
      throw e;
    }
  }

  /**
   * Updates a domain. The domain row is locked before the updater runs, so the updater is given the
   * current domain and concurrent alters apply one after another instead of failing on each other.
   * The write still compares the locked version as a backstop.
   *
   * <p>The updater runs inside this transaction, after the metalake is locked in shared mode and
   * while the domain row is locked. Any lock the updater takes, such as on the backing tag, must
   * follow the order metalake, then domains, then tags.
   *
   * <p>The name is resolved before the row is locked. If the domain is dropped and a new one is
   * created with the same name in between, this call reports the domain as missing instead of
   * changing the new domain; a later call with the name acts on the new domain.
   *
   * <p>The returned domain is read back in the same transaction, so its backing tag name comes from
   * the tag it is now bound to rather than from the updater's entity. The parent cannot be changed
   * by an update, an updater that changes it is rejected; {@link DomainRelService} changes it.
   *
   * @param identifier The domain identifier.
   * @param updater The function that returns the updated domain.
   * @param <E> The entity type.
   * @return The updated domain, as stored.
   * @throws IOException If the domain cannot be stored.
   * @throws NoSuchEntityException If the domain does not exist.
   * @throws EntityAlreadyExistsException If the domain is renamed to a name that is in use.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateDomain")
  public <E extends Entity & HasIdentifier> DomainEntity updateDomain(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    String metalakeName = identifier.namespace().level(0);

    try {
      DomainPO observedDomainPO = getDomainPOByMetalakeAndName(metalakeName, identifier.name());
      DomainPO[] storedDomainPO = new DomainPO[1];
      SessionUtils.doWithCommit(
          DomainMetaMapper.class,
          mapper -> {
            // The metalake fence comes before the domain row, as in every domain write: the
            // updater may lock tag rows, and dropping the metalake locks tags before domains.
            lockMetalakeForDomainWrite(metalakeName, observedDomainPO.metalakeId());
            lockAndGetDomainPO(identifier, observedDomainPO);
            // The locking read cannot join the tag table, so the locked row is read again with the
            // name of its backing tag.
            DomainPO lockedDomainPO = mapper.selectDomainMetaById(observedDomainPO.domainId());
            DomainEntity oldDomainEntity =
                fromDomainPOWithParent(lockedDomainPO, identifier.namespace());
            DomainEntity updatedDomainEntity = (DomainEntity) updater.apply((E) oldDomainEntity);
            Preconditions.checkArgument(
                Objects.equals(oldDomainEntity.id(), updatedDomainEntity.id()),
                "The updated domain entity id: %s must have the same id as the old entity id %s",
                updatedDomainEntity.id(),
                oldDomainEntity.id());
            Preconditions.checkArgument(
                Objects.equals(oldDomainEntity.parent(), updatedDomainEntity.parent()),
                "The parent of domain %s cannot be changed by an update, use DomainRelService",
                identifier);

            DomainPO newDomainPO = DomainPO.updateDomainPO(lockedDomainPO, updatedDomainEntity);
            updateDomainWithVersion(identifier, mapper, lockedDomainPO, newDomainPO);
            storedDomainPO[0] = mapper.selectDomainMetaById(lockedDomainPO.domainId());
          });

      return fromDomainPOWithParent(storedDomainPO[0], identifier.namespace());
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.DOMAIN, identifier.toString());
      throw e;
    }
  }

  /**
   * Deletes a domain, together with its parent edge, its owner and the privileges granted on it.
   * The backing tag and its assignments are kept. A domain with sub-domains is not deleted.
   *
   * @param identifier The domain identifier.
   * @return True if the domain is deleted, false if it does not exist.
   * @throws NonEmptyEntityException If the domain has sub-domains.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteDomain")
  public boolean deleteDomain(NameIdentifier identifier) {
    DomainPO domainPO;
    try {
      domainPO = getDomainPOByMetalakeAndName(identifier.namespace().level(0), identifier.name());
    } catch (NoSuchEntityException e) {
      return false;
    }
    return deleteDomain(identifier, domainPO);
  }

  /**
   * Hard-deletes the domains and the parent edges soft-deleted before the given time.
   *
   * @param legacyTimeline The time before which soft-deleted rows are removed.
   * @param limit The maximum number of rows to remove from each table.
   * @return The number of removed rows.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteDomainMetasByLegacyTimeline")
  public int deleteDomainMetasByLegacyTimeline(long legacyTimeline, int limit) {
    int[] domainDeletedCount = new int[] {0};
    int[] domainRelDeletedCount = new int[] {0};

    SessionUtils.doMultipleWithCommit(
        () ->
            domainDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    DomainMetaMapper.class,
                    mapper -> mapper.deleteDomainMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            domainRelDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    DomainRelMapper.class,
                    mapper -> mapper.deleteDomainRelsByLegacyTimeline(legacyTimeline, limit)));

    return domainDeletedCount[0] + domainRelDeletedCount[0];
  }

  /**
   * Gets the ID of a domain.
   *
   * @param metalakeId The metalake ID.
   * @param domainName The domain name.
   * @return The domain ID.
   * @throws NoSuchEntityException If the domain does not exist.
   */
  public Long getDomainIdByMetalakeIdAndName(Long metalakeId, String domainName) {
    DomainPO domainPO =
        SessionUtils.getWithoutCommit(
            DomainMetaMapper.class,
            mapper -> mapper.selectDomainMetaByMetalakeIdAndName(metalakeId, domainName));

    if (domainPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.DOMAIN.name().toLowerCase(),
          domainName);
    }
    return domainPO.domainId();
  }

  /**
   * Deletes the domain the caller observed. The domain row is locked first, so a domain that was
   * only altered since it was read is still deleted, and the dependent rows are cleaned up in the
   * same transaction as the domain row itself. If another transaction deletes or renames the
   * observed domain first, this method returns {@code false}, also when a new domain has taken the
   * name meanwhile.
   *
   * <p>The observed row is a parameter so that a test can hand in a stale one; production callers
   * use {@link #deleteDomain(NameIdentifier)}, which reads it first.
   */
  @VisibleForTesting
  boolean deleteDomain(NameIdentifier identifier, DomainPO domainPO) {
    long domainId = domainPO.domainId();

    // The metalake lock comes first. Dropping the metalake soft-deletes the privileges before the
    // domains, while this delete changes the domain row before its privileges; without the lock
    // the two could deadlock on a domain with privileges. The domain row is locked next, which also
    // keeps a sub-domain from being attached while the sub-domain check runs. A row that is gone or
    // renamed makes the delete report the domain as missing before any dependent row is touched.
    // The version-checked delete keeps the version as a backstop. The cleanups that follow are
    // blanket soft deletes for this domain ID, so their row counts carry no information and are not
    // checked.
    try {
      SessionUtils.doMultipleWithCommit(
          () -> lockMetalakeForDomainWrite(identifier.namespace().level(0), domainPO.metalakeId()),
          () -> deleteDomainWithVersion(identifier, lockAndGetDomainPO(identifier, domainPO)),
          () ->
              DomainRelService.getInstance().deleteDomainRelsOfDeletedDomain(identifier, domainId),
          () ->
              SessionUtils.doWithoutCommit(
                  OwnerMetaMapper.class,
                  mapper ->
                      mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                          domainId, MetadataObject.Type.DOMAIN.name())),
          () ->
              SessionUtils.doWithoutCommit(
                  SecurableObjectMapper.class,
                  mapper ->
                      mapper.softDeleteObjectRelsByMetadataObject(
                          domainId, MetadataObject.Type.DOMAIN.name())));
    } catch (NoSuchEntityException e) {
      return false;
    }

    return true;
  }

  /**
   * Holds the parent metalake row for the rest of the transaction, so a domain cannot be created,
   * altered or deleted while the metalake is being dropped. The lock is shared, so domains under
   * the same metalake can be written at the same time, while dropping the metalake takes an
   * exclusive lock on the row. A metalake that is gone or renamed is reported as missing.
   */
  private void lockMetalakeForDomainWrite(String metalakeName, Long metalakeId) {
    OccWriteSupport.lockParentForChildWrite(
        metalakeName,
        Entity.EntityType.METALAKE,
        () ->
            SessionUtils.getWithoutCommit(
                MetalakeMetaMapper.class,
                mapper -> mapper.selectMetalakeMetaByIdForShare(metalakeId)),
        null,
        current -> Objects.equals(current.getMetalakeName(), metalakeName));
  }

  /**
   * Writes a new domain or replaces the active domain selected by name or stable ID. The overwrite
   * path uses locking reads instead of a database-specific upsert, which keeps the stored domain ID
   * and version stable across all databases.
   */
  private void insertDomainWithoutCommit(
      DomainEntity domainEntity, DomainPO initializedDomainPO, boolean overwritten) {
    if (!overwritten) {
      insertNewDomainWithoutCommit(initializedDomainPO);
      return;
    }

    DomainPO existingDomainPO = findAndLockDomainForOverwrite(initializedDomainPO);
    if (existingDomainPO == null) {
      if (SessionUtils.getWithoutCommit(
              DomainMetaMapper.class,
              mapper -> mapper.countDeletedDomainMetasById(initializedDomainPO.domainId()))
          > 0) {
        throw new EntityAlreadyExistsException(
            "The domain ID %s is reserved by a deleted domain; use a new ID",
            initializedDomainPO.domainId());
      }
      insertNewDomainWithoutCommit(initializedDomainPO);
      return;
    }

    DomainPO replacementDomainPO = DomainPO.updateDomainPO(existingDomainPO, domainEntity);
    NameIdentifier observedIdentifier =
        NameIdentifier.of(domainEntity.namespace(), existingDomainPO.domainName());
    SessionUtils.doWithoutCommit(
        DomainMetaMapper.class,
        mapper ->
            updateDomainWithVersion(
                observedIdentifier, mapper, existingDomainPO, replacementDomainPO));
  }

  private void insertNewDomainWithoutCommit(DomainPO domainPO) {
    SessionUtils.doWithoutCommit(
        DomainMetaMapper.class, mapper -> mapper.insertDomainMeta(domainPO));
  }

  private DomainPO findAndLockDomainForOverwrite(DomainPO initializedDomainPO) {
    return OccWriteSupport.findAndLockForOverwrite(
        () ->
            SessionUtils.getWithoutCommit(
                DomainMetaMapper.class,
                mapper ->
                    mapper.selectDomainMetaByMetalakeIdAndNameForUpdate(
                        initializedDomainPO.metalakeId(), initializedDomainPO.domainName())),
        () ->
            SessionUtils.getWithoutCommit(
                DomainMetaMapper.class,
                mapper -> mapper.selectDomainMetaByIdForUpdate(initializedDomainPO.domainId())),
        current -> Objects.equals(current.metalakeId(), initializedDomainPO.metalakeId()));
  }

  /**
   * Locks the row of the domain the caller observed and returns it. The row is the same domain only
   * if it still has the observed name and metalake; otherwise the domain is reported as missing.
   * The returned row does not carry the backing tag name.
   */
  private DomainPO lockAndGetDomainPO(NameIdentifier identifier, DomainPO observedDomainPO) {
    return OccWriteSupport.lockParentForChildWrite(
        identifier.name(),
        Entity.EntityType.DOMAIN,
        () ->
            SessionUtils.getWithoutCommit(
                DomainMetaMapper.class,
                mapper -> mapper.selectDomainMetaByIdForUpdate(observedDomainPO.domainId())),
        null,
        current ->
            Objects.equals(current.domainName(), observedDomainPO.domainName())
                && Objects.equals(current.metalakeId(), observedDomainPO.metalakeId()));
  }

  private void updateDomainWithVersion(
      NameIdentifier identifier,
      DomainMetaMapper mapper,
      DomainPO oldDomainPO,
      DomainPO newDomainPO) {
    OccWriteSupport.updateWithVersion(
        () -> mapper.updateDomainMeta(newDomainPO, oldDomainPO),
        () -> domainWriteFailure(identifier, oldDomainPO));
  }

  private void deleteDomainWithVersion(NameIdentifier identifier, DomainPO observedDomainPO) {
    OccWriteSupport.deleteWithVersion(
        () ->
            SessionUtils.getWithoutCommit(
                DomainMetaMapper.class,
                mapper ->
                    mapper.softDeleteDomainMetaByIdAndVersion(
                        observedDomainPO.domainId(), observedDomainPO.currentVersion())),
        () -> domainWriteFailure(identifier, observedDomainPO));
  }

  private RuntimeException domainWriteFailure(
      NameIdentifier identifier, DomainPO observedDomainPO) {
    return OccWriteSupport.writeFailure(
        identifier,
        Entity.EntityType.DOMAIN,
        () ->
            SessionUtils.getWithoutCommit(
                DomainMetaMapper.class,
                mapper -> mapper.selectDomainMetaByIdForUpdate(observedDomainPO.domainId())),
        null,
        current ->
            Objects.equals(current.domainName(), observedDomainPO.domainName())
                && Objects.equals(current.metalakeId(), observedDomainPO.metalakeId()));
  }

  private DomainPO getDomainPOByMetalakeAndName(String metalakeName, String domainName) {
    DomainPO domainPO =
        SessionUtils.getWithoutCommit(
            DomainMetaMapper.class,
            mapper -> mapper.selectDomainMetaByMetalakeAndName(metalakeName, domainName));

    if (domainPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.DOMAIN.name().toLowerCase(),
          domainName);
    }
    return domainPO;
  }

  private DomainEntity fromDomainPOWithParent(DomainPO domainPO, Namespace namespace) {
    return DomainPO.fromDomainPO(
        domainPO,
        namespace,
        DomainRelService.getInstance().getParentDomainName(domainPO.domainId()).orElse(null));
  }

  private List<DomainEntity> fromDomainPOsWithParents(
      List<DomainPO> domainPOs, Namespace namespace) {
    List<Long> domainIds = domainPOs.stream().map(DomainPO::domainId).collect(Collectors.toList());
    return DomainPO.fromDomainPOs(
        domainPOs, namespace, DomainRelService.getInstance().batchGetParentDomainNames(domainIds));
  }
}
