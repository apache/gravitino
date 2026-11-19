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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsDomainOperations.LockedDomainState;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.DomainMetaMapper;
import org.apache.gravitino.storage.relational.mapper.DomainRelMapper;
import org.apache.gravitino.storage.relational.po.DomainPO;
import org.apache.gravitino.storage.relational.po.DomainRelPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/**
 * The service that stores the parent edge of sub-domains in the {@code domain_rel} table. A domain
 * has at most one active edge, enforced by the unique key on {@code (child_id, deleted_at)}.
 *
 * <p>For parent-name-only lookups, this service provides the shared resolver through {@link
 * #getParentDomainName(Long)} and {@link #batchGetParentDomainNames(List)}. Future inherited
 * authorization checks can reuse these methods. Domain entity reads instead join the live parent
 * into the same query as the domain metadata, preserving a consistent statement snapshot without a
 * separate resolver lookup.
 *
 * <p>Every change of the hierarchy first locks the metalake row, then locks the {@code domain_meta}
 * rows of the domains involved in domain ID order: the sub-domain exclusively and the parent
 * shared. Taking the metalake first keeps the order of a metalake drop, which locks the metalake
 * row exclusively before its domain rows, so the two cannot deadlock. All writers of an edge lock
 * the sub-domain, so the edge read under that lock is current. Each locked row is re-read and must
 * still have the observed name, otherwise the change fails as if it did not exist.
 *
 * <p>Status changes are made here too, since the status of a domain must agree with its parent and
 * sub-domains: they lock the domain exclusively and its current parent shared, in the same order.
 *
 * <p>This service does not enforce the shape of the hierarchy or the status rules. The changes that
 * set a parent or a status take a check from the caller, which runs on the domains as they are
 * under the locks; the domain manager's checks keep the hierarchy one level deep, which also keeps
 * it acyclic.
 */
public class DomainRelService {

  private static final DomainRelService INSTANCE = new DomainRelService();

  private static final int MAX_STATUS_UPDATE_ATTEMPTS = 3;

  private DomainRelService() {}

  /**
   * Returns the singleton service instance.
   *
   * @return The domain relation service.
   */
  public static DomainRelService getInstance() {
    return INSTANCE;
  }

  /**
   * Returns the name of the current parent of a domain.
   *
   * @param domainId The domain ID.
   * @return The name of the parent domain, or empty if the domain is top-level.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getParentDomainName")
  public Optional<String> getParentDomainName(Long domainId) {
    return Optional.ofNullable(
        batchGetParentDomainNames(Collections.singletonList(domainId)).get(domainId));
  }

  /**
   * Returns the names of the current parents of the given domains.
   *
   * @param domainIds The domain IDs.
   * @return The name of the parent of each domain that has one, keyed by domain ID. Top-level
   *     domains are not in the map.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchGetParentDomainNames")
  public Map<Long, String> batchGetParentDomainNames(List<Long> domainIds) {
    if (domainIds.isEmpty()) {
      return Collections.emptyMap();
    }
    List<DomainRelPO> domainRelPOs =
        SessionUtils.getWithoutCommit(
            DomainRelMapper.class, mapper -> mapper.listDomainRelsByChildIds(domainIds));
    return domainRelPOs.stream()
        .collect(Collectors.toMap(DomainRelPO::childId, DomainRelPO::parentName));
  }

  /**
   * Lists the sub-domains of a domain.
   *
   * @param parentIdent The identifier of the parent domain.
   * @return The sub-domains, each with the parent set.
   * @throws NoSuchEntityException If the parent domain does not exist.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listSubDomains")
  public List<DomainEntity> listSubDomains(NameIdentifier parentIdent) {
    List<DomainPO> subDomainPOs =
        SessionUtils.getWithoutCommit(
            DomainMetaMapper.class,
            mapper ->
                mapper.listDomainPOsByParentIdentifier(
                    parentIdent.namespace().level(0), parentIdent.name()));
    if (subDomainPOs.isEmpty()) {
      throw new NoSuchEntityException("The domain %s does not exist", parentIdent);
    }
    return subDomainPOs.stream()
        .filter(po -> po.domainId() != null)
        .map(po -> DomainPO.fromDomainPO(po, parentIdent.namespace()))
        .collect(Collectors.toList());
  }

  /**
   * Sets the parent of a domain: inserts its parent edge if it is top-level, or re-points the edge
   * if it is a sub-domain. Setting the current parent changes nothing.
   *
   * <p>Whether the edge is inserted or re-pointed is decided under the lock on the domain, so a
   * concurrent attach or detach of the same domain cannot make the change fail. The check gets the
   * domain with its current parent, the new parent with its own parent, and the sub-domains of the
   * domain, all read under the locks.
   *
   * @param childIdent The identifier of the domain.
   * @param parentIdent The identifier of the new parent domain, in the same metalake.
   * @param auditInfo The audit info of the change. A new edge is created by its creator; a
   *     re-pointed edge keeps its creator and takes its last modifier.
   * @param check The check of the locked state; the change is rolled back if it throws.
   * @return The domain after the change.
   * @throws IOException If the edge cannot be stored.
   * @throws NoSuchEntityException If either domain does not exist.
   * @throws IllegalArgumentException If the parent is the domain itself or is in another metalake.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "setDomainParentAndGet")
  public DomainEntity setDomainParentAndGet(
      NameIdentifier childIdent,
      NameIdentifier parentIdent,
      AuditInfo auditInfo,
      Consumer<LockedDomainState> check)
      throws IOException {
    checkSameMetalakeAndDistinct(childIdent, parentIdent);
    DomainEntity[] changed = new DomainEntity[1];
    try {
      DomainPO childPO = getDomainPO(childIdent);
      DomainPO parentPO = getDomainPO(parentIdent);
      checkDistinctRows(childIdent, childPO, parentPO);
      SessionUtils.doMultipleWithCommit(
          () -> {
            Pair<DomainPO, DomainPO> locked = lockMetalakeAndDomains(childIdent, childPO, parentPO);
            Namespace namespace = childIdent.namespace();
            DomainRelPO oldDomainRelPO = selectDomainRel(childPO.domainId());
            check.accept(
                new LockedDomainState(
                    toEntityWithParent(locked.getLeft(), namespace),
                    toEntityWithParent(locked.getRight(), namespace),
                    listSubDomainsOf(locked.getLeft(), namespace)));

            if (oldDomainRelPO == null) {
              DomainRelPO domainRelPO =
                  DomainRelPO.initializeDomainRelPO(
                      childPO.metalakeId(), parentPO.domainId(), childPO.domainId(), auditInfo);
              SessionUtils.doWithoutCommit(
                  DomainRelMapper.class, mapper -> mapper.insertDomainRel(domainRelPO));
            } else if (!Objects.equals(oldDomainRelPO.parentId(), parentPO.domainId())) {
              DomainRelPO newDomainRelPO =
                  DomainRelPO.updateDomainRelPO(oldDomainRelPO, parentPO.domainId(), auditInfo);
              OccWriteSupport.updateWithVersion(
                  () ->
                      SessionUtils.getWithoutCommit(
                          DomainRelMapper.class,
                          mapper -> mapper.updateDomainRel(newDomainRelPO, oldDomainRelPO)),
                  () ->
                      ExceptionUtils.concurrentModification(Entity.EntityType.DOMAIN, childIdent));
            }
            changed[0] = readDomain(childPO.domainId(), namespace);
          });
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.DOMAIN, childIdent.toString());
      throw e;
    }
    return changed[0];
  }

  /**
   * Removes the parent edge of a domain, which makes it top-level, and returns the domain as it is
   * after the change. Removing the parent of a top-level domain changes nothing.
   *
   * <p>The check gets the domain with its current parent, read under the exclusive lock on the
   * domain, which every writer of its edge holds; so that parent is the one the edge is removed
   * from.
   *
   * @param childIdent The identifier of the domain.
   * @param check The check of the locked state; the change is rolled back if it throws.
   * @return The domain after the change.
   * @throws IOException If the edge cannot be removed.
   * @throws NoSuchEntityException If the domain does not exist.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "removeDomainParentAndGet")
  public DomainEntity removeDomainParentAndGet(
      NameIdentifier childIdent, Consumer<LockedDomainState> check) throws IOException {
    DomainEntity[] changed = new DomainEntity[1];
    try {
      DomainPO childPO = getDomainPO(childIdent);
      SessionUtils.doMultipleWithCommit(
          () -> {
            DomainPO lockedPO = lockMetalakeAndDomains(childIdent, childPO, null).getLeft();
            check.accept(
                new LockedDomainState(
                    toEntityWithParent(lockedPO, childIdent.namespace()),
                    null,
                    Collections.emptyList()));
            softDeleteDomainRel(childPO.domainId());
            changed[0] = readDomain(childPO.domainId(), childIdent.namespace());
          });
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.DOMAIN, childIdent.toString());
      throw e;
    }
    return changed[0];
  }

  /**
   * Sets the status of a domain. Setting the current status writes nothing.
   *
   * <p>The domain is locked exclusively and its current parent shared, so until the change commits
   * the parent's status and the domain's parent cannot change, and no sub-domain can be added to
   * the domain or change its status: every writer of them locks one of these rows in a conflicting
   * mode. A sub-domain can still be detached or deleted, which only removes it. The parent decides
   * which rows to lock, but it is read before they are locked. Once the locks are held, the parent
   * is read again, and if it changed meanwhile, the change is rolled back and retried with the new
   * parent, a bounded number of times.
   *
   * <p>A retry must start a new transaction, so this method cannot run inside one.
   *
   * @param identifier The identifier of the domain.
   * @param status The new status.
   * @param auditInfo The audit info of the change; the domain keeps its creator and takes its last
   *     modifier.
   * @param check The check of the locked state; the change is rolled back if it throws.
   * @return The domain after the change.
   * @throws IOException If the domain cannot be stored.
   * @throws NoSuchEntityException If the domain does not exist.
   * @throws OptimisticLockException If the parent kept changing until the retries ran out.
   * @throws IllegalStateException If it is called inside a transaction.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateDomainStatusAndGet")
  public DomainEntity updateDomainStatusAndGet(
      NameIdentifier identifier,
      Domain.Status status,
      AuditInfo auditInfo,
      Consumer<LockedDomainState> check)
      throws IOException {
    Preconditions.checkState(
        !SessionUtils.isInTransaction(),
        "The status of domain %s cannot be changed inside another transaction",
        identifier);
    for (int attempt = 0; attempt < MAX_STATUS_UPDATE_ATTEMPTS; attempt++) {
      DomainPO observedPO = getDomainPO(identifier);
      DomainRelPO observedRelPO = selectDomainRel(observedPO.domainId());
      Optional<DomainEntity> updated =
          tryUpdateDomainStatusOnce(
              identifier, status, auditInfo, check, observedPO, observedRelPO);
      if (updated.isPresent()) {
        return updated.get();
      }
    }
    throw ExceptionUtils.concurrentModification(Entity.EntityType.DOMAIN, identifier);
  }

  /**
   * Sets the status of a domain once, locking the parent the caller observed.
   *
   * @return The domain after the change, or empty if the parent of the domain is no longer the
   *     observed one; nothing is written then.
   */
  @VisibleForTesting
  Optional<DomainEntity> tryUpdateDomainStatusOnce(
      NameIdentifier identifier,
      Domain.Status status,
      AuditInfo auditInfo,
      Consumer<LockedDomainState> check,
      DomainPO observedPO,
      @Nullable DomainRelPO observedRelPO)
      throws IOException {
    Long observedParentId = observedRelPO == null ? null : observedRelPO.parentId();
    Namespace namespace = identifier.namespace();
    DomainEntity[] updated = new DomainEntity[1];
    try {
      SessionUtils.doMultipleWithCommit(
          () -> {
            Pair<DomainPO, DomainPO> locked =
                lockMetalakeDomainAndParent(identifier, observedPO, observedParentId);
            DomainRelPO lockedRelPO = selectDomainRel(observedPO.domainId());
            Long lockedParentId = lockedRelPO == null ? null : lockedRelPO.parentId();
            if (!Objects.equals(lockedParentId, observedParentId) || locked == null) {
              throw new ParentChangedException();
            }

            DomainPO lockedPO = locked.getLeft();
            DomainEntity domain = toEntityWithParent(lockedPO, namespace);
            check.accept(
                new LockedDomainState(
                    domain,
                    locked.getRight() == null
                        ? null
                        : toEntityWithParent(locked.getRight(), namespace),
                    listSubDomainsOf(lockedPO, namespace)));

            if (domain.status() != status) {
              DomainEntity newDomain =
                  domain.toBuilder()
                      .withStatus(status)
                      .withAuditInfo(
                          AuditInfo.builder()
                              .withCreator(domain.auditInfo().creator())
                              .withCreateTime(domain.auditInfo().createTime())
                              .withLastModifier(auditInfo.lastModifier())
                              .withLastModifiedTime(auditInfo.lastModifiedTime())
                              .build())
                      .build();
              DomainPO newPO = DomainPO.updateDomainPO(lockedPO, newDomain);
              OccWriteSupport.updateWithVersion(
                  () ->
                      SessionUtils.getWithoutCommit(
                          DomainMetaMapper.class,
                          mapper -> mapper.updateDomainMeta(newPO, lockedPO)),
                  () ->
                      ExceptionUtils.concurrentModification(Entity.EntityType.DOMAIN, identifier));
            }
            updated[0] = readDomain(observedPO.domainId(), namespace);
          });
    } catch (ParentChangedException e) {
      return Optional.empty();
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.DOMAIN, identifier.toString());
      throw e;
    }
    return Optional.of(updated[0]);
  }

  /**
   * Inserts the parent edge of a top-level domain, which makes it a sub-domain of {@code
   * parentIdent}. It takes no check of the locked state; production code sets a parent with {@link
   * #setDomainParentAndGet}, so that the hierarchy rules are checked.
   *
   * @param childIdent The identifier of the domain.
   * @param parentIdent The identifier of the parent domain, in the same metalake.
   * @param auditInfo The audit info of the edge.
   * @throws IOException If the edge cannot be stored.
   * @throws NoSuchEntityException If either domain does not exist.
   * @throws EntityAlreadyExistsException If the domain already has a parent.
   * @throws IllegalArgumentException If the parent is the domain itself or is in another metalake.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertDomainRel")
  @VisibleForTesting
  void insertDomainRel(NameIdentifier childIdent, NameIdentifier parentIdent, AuditInfo auditInfo)
      throws IOException {
    checkSameMetalakeAndDistinct(childIdent, parentIdent);
    try {
      DomainPO childPO = getDomainPO(childIdent);
      DomainPO parentPO = getDomainPO(parentIdent);
      checkDistinctRows(childIdent, childPO, parentPO);
      SessionUtils.doMultipleWithCommit(
          () -> lockMetalakeAndDomains(childIdent, childPO, parentPO),
          () -> {
            if (selectDomainRel(childPO.domainId()) != null) {
              throw new EntityAlreadyExistsException(
                  "The domain %s already has a parent", childIdent);
            }
            DomainRelPO domainRelPO =
                DomainRelPO.initializeDomainRelPO(
                    childPO.metalakeId(), parentPO.domainId(), childPO.domainId(), auditInfo);
            SessionUtils.doWithoutCommit(
                DomainRelMapper.class, mapper -> mapper.insertDomainRel(domainRelPO));
          });
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.DOMAIN, childIdent.toString());
      throw e;
    }
  }

  /**
   * Re-points the parent edge of a sub-domain to another parent. Re-pointing it to its current
   * parent changes nothing. It takes no check of the locked state; production code sets a parent
   * with {@link #setDomainParentAndGet}, so that the hierarchy rules are checked.
   *
   * @param childIdent The identifier of the sub-domain.
   * @param newParentIdent The identifier of the new parent domain, in the same metalake.
   * @param auditInfo The audit info of the change; the edge keeps its creator.
   * @throws IOException If the edge cannot be stored.
   * @throws NoSuchEntityException If either domain does not exist, or the domain has no parent.
   * @throws IllegalArgumentException If the new parent is the domain itself or is in another
   *     metalake.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateDomainRel")
  @VisibleForTesting
  void updateDomainRel(
      NameIdentifier childIdent, NameIdentifier newParentIdent, AuditInfo auditInfo)
      throws IOException {
    checkSameMetalakeAndDistinct(childIdent, newParentIdent);
    try {
      DomainPO childPO = getDomainPO(childIdent);
      DomainPO newParentPO = getDomainPO(newParentIdent);
      checkDistinctRows(childIdent, childPO, newParentPO);
      SessionUtils.doMultipleWithCommit(
          () -> lockMetalakeAndDomains(childIdent, childPO, newParentPO),
          () -> {
            DomainRelPO oldDomainRelPO = selectDomainRel(childPO.domainId());
            if (oldDomainRelPO == null) {
              throw new NoSuchEntityException("The domain %s has no parent", childIdent);
            }
            if (Objects.equals(oldDomainRelPO.parentId(), newParentPO.domainId())) {
              return;
            }
            DomainRelPO newDomainRelPO =
                DomainRelPO.updateDomainRelPO(oldDomainRelPO, newParentPO.domainId(), auditInfo);
            OccWriteSupport.updateWithVersion(
                () ->
                    SessionUtils.getWithoutCommit(
                        DomainRelMapper.class,
                        mapper -> mapper.updateDomainRel(newDomainRelPO, oldDomainRelPO)),
                () -> ExceptionUtils.concurrentModification(Entity.EntityType.DOMAIN, childIdent));
          });
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.DOMAIN, childIdent.toString());
      throw e;
    }
  }

  /**
   * Locks the parent of a new sub-domain shared, in the caller's transaction, after the caller
   * locked the metalake, and returns it with its own parent set.
   *
   * @param parentIdent The identifier of the parent domain.
   * @return The locked parent domain.
   * @throws NoSuchEntityException If the parent domain does not exist.
   */
  DomainEntity lockAndGetParentOfNewSubDomain(NameIdentifier parentIdent) {
    DomainPO lockedParentPO = lockDomain(getDomainPO(parentIdent), false);
    SessionUtils.clearSessionCache();
    return toEntityWithParent(lockedParentPO, parentIdent.namespace());
  }

  /**
   * Locks the metalake shared and a domain exclusively, in the caller's transaction, for a change
   * of the domain row that involves no other domain, and returns the domain row as it is under the
   * lock.
   *
   * @param identifier The identifier of the domain.
   * @param observedPO The domain row the caller read before locking.
   * @return The locked domain row.
   * @throws NoSuchEntityException If the domain was deleted or renamed after it was read.
   */
  DomainPO lockAndGetDomainForChange(NameIdentifier identifier, DomainPO observedPO) {
    return lockMetalakeAndDomains(identifier, observedPO, null).getLeft();
  }

  // The parent name is read under the lock on the domain, which every writer of its edge holds.
  DomainEntity toEntityWithParent(DomainPO domainPO, Namespace namespace) {
    return DomainPO.fromDomainPO(
        domainPO, namespace, getParentDomainName(domainPO.domainId()).orElse(null));
  }

  /**
   * Inserts the parent edge of a new sub-domain, in the caller's transaction. The caller holds the
   * shared lock on the parent from {@link #lockAndGetParentOfNewSubDomain(NameIdentifier)}.
   *
   * @param metalakeId The metalake ID.
   * @param parentId The ID of the parent domain.
   * @param childId The ID of the new sub-domain.
   * @param auditInfo The audit info of the edge.
   */
  void insertDomainRelOfNewSubDomain(
      Long metalakeId, Long parentId, Long childId, AuditInfo auditInfo) {
    DomainRelPO domainRelPO =
        DomainRelPO.initializeDomainRelPO(metalakeId, parentId, childId, auditInfo);
    SessionUtils.doWithoutCommit(
        DomainRelMapper.class, mapper -> mapper.insertDomainRel(domainRelPO));
  }

  /**
   * Checks that a domain being deleted has no sub-domains, in the caller's transaction. The caller
   * must hold the exclusive lock on the domain row, so no sub-domain can be attached to it
   * meanwhile.
   *
   * <p>The sub-domain count is a plain read. It still sees every edge attached before the lock was
   * granted, because the store runs at read committed, where each statement reads the data
   * committed before it starts.
   *
   * @param identifier The identifier of the domain.
   * @param domainId The domain ID.
   * @throws NonEmptyEntityException If the domain has sub-domains.
   */
  void checkDomainHasNoSubDomains(NameIdentifier identifier, long domainId) {
    int subDomainCount =
        SessionUtils.getWithoutCommit(
            DomainRelMapper.class, mapper -> mapper.countDomainRelsByParentId(domainId));
    if (subDomainCount > 0) {
      throw new NonEmptyEntityException(
          "The domain %s has %d sub-domain(s), remove them first", identifier, subDomainCount);
    }
  }

  // Returns the locked child and the locked parent, which is null without a parent.
  private Pair<DomainPO, DomainPO> lockMetalakeAndDomains(
      NameIdentifier childIdent, DomainPO childPO, @Nullable DomainPO parentPO) {
    MetalakeMetaService.getInstance()
        .lockMetalakeForChildWrite(childIdent.namespace().level(0), childPO.metalakeId());
    Pair<DomainPO, DomainPO> locked;
    if (parentPO == null) {
      locked = Pair.of(lockDomain(childPO, true), null);
    } else if (childPO.domainId() < parentPO.domainId()) {
      DomainPO lockedChildPO = lockDomain(childPO, true);
      locked = Pair.of(lockedChildPO, lockDomain(parentPO, false));
    } else {
      DomainPO lockedParentPO = lockDomain(parentPO, false);
      locked = Pair.of(lockDomain(childPO, true), lockedParentPO);
    }
    SessionUtils.clearSessionCache();
    return locked;
  }

  // Like lockMetalakeAndDomains, but the parent is locked by ID only: whether it is still the
  // parent is decided by the edge read afterwards, not by its name. Returns null if the parent row
  // is gone, which means that the edge changed.
  @Nullable
  private Pair<DomainPO, DomainPO> lockMetalakeDomainAndParent(
      NameIdentifier identifier, DomainPO observedPO, @Nullable Long parentId) {
    MetalakeMetaService.getInstance()
        .lockMetalakeForChildWrite(identifier.namespace().level(0), observedPO.metalakeId());
    if (parentId == null) {
      return Pair.of(lockDomain(observedPO, true), null);
    }
    DomainPO lockedPO;
    DomainPO lockedParentPO;
    if (observedPO.domainId() < parentId) {
      lockedPO = lockDomain(observedPO, true);
      lockedParentPO = selectDomainForShare(parentId);
    } else {
      lockedParentPO = selectDomainForShare(parentId);
      lockedPO = lockDomain(observedPO, true);
    }
    SessionUtils.clearSessionCache();
    return lockedParentPO == null ? null : Pair.of(lockedPO, lockedParentPO);
  }

  @Nullable
  private DomainPO selectDomainForShare(Long domainId) {
    return SessionUtils.getWithoutCommit(
        DomainMetaMapper.class, mapper -> mapper.selectDomainMetaByIdForShare(domainId));
  }

  private List<DomainEntity> listSubDomainsOf(DomainPO domainPO, Namespace namespace) {
    return SessionUtils.getWithoutCommit(
            DomainMetaMapper.class,
            mapper ->
                mapper.listDomainPOsByParentIdentifier(namespace.level(0), domainPO.domainName()))
        .stream()
        .filter(po -> po.domainId() != null)
        .map(po -> DomainPO.fromDomainPO(po, namespace))
        .collect(Collectors.toList());
  }

  // Reads the domain with its backing tag name, in the caller's transaction.
  private DomainEntity readDomain(Long domainId, Namespace namespace) {
    DomainPO domainPO =
        SessionUtils.getWithoutCommit(
            DomainMetaMapper.class, mapper -> mapper.selectDomainMetaById(domainId));
    return toEntityWithParent(domainPO, namespace);
  }

  private DomainPO lockDomain(DomainPO observedDomainPO, boolean exclusive) {
    return OccWriteSupport.lockParentForChildWrite(
        observedDomainPO.domainName(),
        Entity.EntityType.DOMAIN,
        () ->
            SessionUtils.getWithoutCommit(
                DomainMetaMapper.class,
                mapper ->
                    exclusive
                        ? mapper.selectDomainMetaByIdForUpdate(observedDomainPO.domainId())
                        : mapper.selectDomainMetaByIdForShare(observedDomainPO.domainId())),
        null,
        current ->
            Objects.equals(current.domainName(), observedDomainPO.domainName())
                && Objects.equals(current.metalakeId(), observedDomainPO.metalakeId()));
  }

  @Nullable
  private DomainRelPO selectDomainRel(Long childId) {
    return SessionUtils.getWithoutCommit(
        DomainRelMapper.class, mapper -> mapper.selectDomainRelByChildId(childId));
  }

  private int softDeleteDomainRel(Long childId) {
    return SessionUtils.getWithoutCommit(
        DomainRelMapper.class, mapper -> mapper.softDeleteDomainRelByChildId(childId));
  }

  private DomainPO getDomainPO(NameIdentifier identifier) {
    DomainPO domainPO =
        SessionUtils.getWithoutCommit(
            DomainMetaMapper.class,
            mapper ->
                mapper.selectDomainMetaByMetalakeAndName(
                    identifier.namespace().level(0), identifier.name()));
    if (domainPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.DOMAIN.name().toLowerCase(),
          identifier.name());
    }
    return domainPO;
  }

  // Two names can resolve to the same row, for example on MySQL, whose collation ignores trailing
  // spaces, so the rows are compared as well as the names.
  private static void checkDistinctRows(
      NameIdentifier childIdent, DomainPO childPO, DomainPO parentPO) {
    Preconditions.checkArgument(
        !Objects.equals(childPO.domainId(), parentPO.domainId()),
        "The domain %s cannot be its own parent",
        childIdent);
  }

  private static void checkSameMetalakeAndDistinct(
      NameIdentifier childIdent, NameIdentifier parentIdent) {
    Preconditions.checkArgument(
        Objects.equals(childIdent.namespace(), parentIdent.namespace()),
        "The domain %s and its parent %s must be in the same metalake",
        childIdent,
        parentIdent);
    Preconditions.checkArgument(
        !Objects.equals(childIdent.name(), parentIdent.name()),
        "The domain %s cannot be its own parent",
        childIdent);
  }

  // Rolls back a status change whose parent changed between the read and the locks.
  private static final class ParentChangedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private ParentChangedException() {
      super(null, null, false, false);
    }
  }
}
