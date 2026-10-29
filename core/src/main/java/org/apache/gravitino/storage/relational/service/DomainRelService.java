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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
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
 * <p>It is also the one parent resolver of domains: domain names carry no parent, so every reader
 * that needs a domain's parent, such as domain reads and the inherited authorization checks, gets
 * it from {@link #getParentDomainName(Long)} or {@link #batchGetParentDomainNames(List)}.
 *
 * <p>Every change of the hierarchy first locks the metalake row, then locks the {@code domain_meta}
 * rows of the domains involved in domain ID order: the sub-domain exclusively and the parent
 * shared. Taking the metalake first keeps the order of a metalake drop, which locks the metalake
 * row exclusively before its domain rows, so the two cannot deadlock. All writers of an edge lock
 * the sub-domain, so the edge read under that lock is current. Each locked row is re-read and must
 * still have the observed name, otherwise the change fails as if it did not exist.
 *
 * <p>The hierarchy stays acyclic. Attaching or re-pointing a domain walks the ancestors of the new
 * parent and is refused if the domain is among them. Two such changes on disjoint domains can still
 * close a cycle together, so they hold the metalake row exclusively, which runs them one at a time
 * within a metalake and lets each walk see the edges of the ones before it. Removing an edge cannot
 * close a cycle and holds the metalake row shared.
 */
public class DomainRelService {

  private static final DomainRelService INSTANCE = new DomainRelService();

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
    DomainPO parentPO = getDomainPO(parentIdent);
    List<DomainPO> subDomainPOs =
        SessionUtils.getWithoutCommit(
            DomainMetaMapper.class, mapper -> mapper.listDomainPOsByParentId(parentPO.domainId()));
    return subDomainPOs.stream()
        .map(po -> DomainPO.fromDomainPO(po, parentIdent.namespace(), parentPO.domainName()))
        .collect(Collectors.toList());
  }

  /**
   * Inserts the parent edge of a top-level domain, which makes it a sub-domain of {@code
   * parentIdent}.
   *
   * @param childIdent The identifier of the domain.
   * @param parentIdent The identifier of the parent domain, in the same metalake.
   * @param auditInfo The audit info of the edge.
   * @throws IOException If the edge cannot be stored.
   * @throws NoSuchEntityException If either domain does not exist.
   * @throws EntityAlreadyExistsException If the domain already has a parent.
   * @throws IllegalArgumentException If the parent is the domain itself, is in another metalake, or
   *     is a sub-domain of the domain at any depth.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertDomainRel")
  public void insertDomainRel(
      NameIdentifier childIdent, NameIdentifier parentIdent, AuditInfo auditInfo)
      throws IOException {
    checkSameMetalakeAndDistinct(childIdent, parentIdent);
    try {
      DomainPO childPO = getDomainPO(childIdent);
      DomainPO parentPO = getDomainPO(parentIdent);
      SessionUtils.doMultipleWithCommit(
          () -> lockMetalakeAndDomains(childIdent, childPO, parentPO),
          () -> {
            if (selectDomainRel(childPO.domainId()) != null) {
              throw new EntityAlreadyExistsException(
                  "The domain %s already has a parent", childIdent);
            }
            checkNotAncestor(childIdent, childPO, parentIdent, parentPO);
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
   * parent changes nothing.
   *
   * @param childIdent The identifier of the sub-domain.
   * @param newParentIdent The identifier of the new parent domain, in the same metalake.
   * @param auditInfo The audit info of the change; the edge keeps its creator.
   * @throws IOException If the edge cannot be stored.
   * @throws NoSuchEntityException If either domain does not exist, or the domain has no parent.
   * @throws IllegalArgumentException If the new parent is the domain itself, is in another
   *     metalake, or is a sub-domain of the domain at any depth.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateDomainRel")
  public void updateDomainRel(
      NameIdentifier childIdent, NameIdentifier newParentIdent, AuditInfo auditInfo)
      throws IOException {
    checkSameMetalakeAndDistinct(childIdent, newParentIdent);
    try {
      DomainPO childPO = getDomainPO(childIdent);
      DomainPO newParentPO = getDomainPO(newParentIdent);
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
            checkNotAncestor(childIdent, childPO, newParentIdent, newParentPO);
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
   * Removes the parent edge of a domain, which makes it top-level.
   *
   * @param childIdent The identifier of the domain.
   * @return True if the edge is removed, false if the domain is already top-level.
   * @throws IOException If the edge cannot be removed.
   * @throws NoSuchEntityException If the domain does not exist.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteDomainRel")
  public boolean deleteDomainRel(NameIdentifier childIdent) throws IOException {
    int[] deleted = new int[1];
    try {
      DomainPO childPO = getDomainPO(childIdent);
      SessionUtils.doMultipleWithCommit(
          () -> lockMetalakeAndDomains(childIdent, childPO, null),
          () -> deleted[0] = softDeleteDomainRel(childPO.domainId()));
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.DOMAIN, childIdent.toString());
      throw e;
    }
    return deleted[0] > 0;
  }

  /**
   * Removes the edges of a domain that is being deleted, in the caller's transaction. The caller
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
  void deleteDomainRelsOfDeletedDomain(NameIdentifier identifier, long domainId) {
    int subDomainCount =
        SessionUtils.getWithoutCommit(
            DomainRelMapper.class, mapper -> mapper.countDomainRelsByParentId(domainId));
    if (subDomainCount > 0) {
      throw new NonEmptyEntityException(
          "The domain %s has %d sub-domain(s), remove them first", identifier, subDomainCount);
    }
    softDeleteDomainRel(domainId);
  }

  // A change that adds an edge to parentPO can close a cycle, so it holds the metalake row
  // exclusively; removing an edge, with no parentPO, holds it shared.
  private void lockMetalakeAndDomains(
      NameIdentifier childIdent, DomainPO childPO, @Nullable DomainPO parentPO) {
    String metalakeName = childIdent.namespace().level(0);
    if (parentPO == null) {
      MetalakeMetaService.getInstance()
          .lockMetalakeForChildWrite(metalakeName, childPO.metalakeId());
    } else {
      MetalakeMetaService.getInstance()
          .lockMetalakeExclusivelyForChildWrite(metalakeName, childPO.metalakeId());
    }
    if (parentPO == null) {
      lockDomain(childPO, true);
    } else if (childPO.domainId() < parentPO.domainId()) {
      lockDomain(childPO, true);
      lockDomain(parentPO, false);
    } else {
      lockDomain(parentPO, false);
      lockDomain(childPO, true);
    }
  }

  private void lockDomain(DomainPO observedDomainPO, boolean exclusive) {
    OccWriteSupport.lockParentForChildWrite(
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

  // Runs under the exclusive metalake lock, so no other change of the hierarchy moves the
  // ancestors while they are walked.
  private void checkNotAncestor(
      NameIdentifier childIdent, DomainPO childPO, NameIdentifier parentIdent, DomainPO parentPO) {
    Set<Long> visitedIds = new HashSet<>();
    Long ancestorId = parentPO.domainId();
    while (ancestorId != null) {
      Preconditions.checkArgument(
          !Objects.equals(ancestorId, childPO.domainId()),
          "The domain %s cannot be a sub-domain of %s, which is its own sub-domain",
          childIdent,
          parentIdent);
      Preconditions.checkState(
          visitedIds.add(ancestorId),
          "The ancestors of domain %s form a cycle at domain ID %s",
          parentIdent,
          ancestorId);
      DomainRelPO ancestorRelPO = selectDomainRel(ancestorId);
      ancestorId = ancestorRelPO == null ? null : ancestorRelPO.parentId();
    }
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
}
