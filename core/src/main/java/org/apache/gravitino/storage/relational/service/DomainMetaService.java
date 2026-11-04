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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsDomainOperations.LockedDomainState;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.TagEntity;
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
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.NamespaceUtil;

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
   * Inserts a top-level domain. A sub-domain, whose parent must be locked and checked, is inserted
   * with {@link #insertDomainWithBackingTag} or {@link #insertDomainWithNewBackingTag}. Replacing a
   * domain keeps its status, its parent and its backing tag, which are changed only under the locks
   * their rules need: the status by {@link DomainRelService#updateDomainStatusAndGet}, the backing
   * tag by {@link #updateDomainBackingTagAndGet}.
   *
   * @param domainEntity The domain to insert.
   * @param overwritten Whether to replace an active domain with the same name or ID.
   * @throws IOException If the domain cannot be stored.
   * @throws EntityAlreadyExistsException If a domain with the same name exists and {@code
   *     overwritten} is false.
   * @throws IllegalArgumentException If the domain has a parent, or replaces a domain with another
   *     status or another backing tag.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertDomain")
  public void insertDomain(DomainEntity domainEntity, boolean overwritten) throws IOException {
    Preconditions.checkArgument(
        !domainEntity.parent().isPresent(),
        "The domain %s has a parent and must be inserted with its parent locked",
        domainEntity.nameIdentifier());
    Namespace ns = domainEntity.namespace();
    String metalakeName = ns.level(0);

    try {
      MetalakePO metalakePO = getMetalakePO(metalakeName);
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
   * Inserts a new domain backed by an existing tag, after a check of the locked state. If the
   * domain has a parent, it is inserted as a sub-domain of that parent, together with its parent
   * edge, in one transaction.
   *
   * <p>The metalake row is locked shared, then the parent row, if any, shared, so the parent cannot
   * be attached elsewhere, deleted or have its status changed until the domain is committed. Then
   * the tag is resolved by name and locked exclusively, so it cannot be deleted or bound by another
   * domain meanwhile; every binding of a tag holds this lock. The domain is written next, so a
   * missing parent or tag is reported before a duplicate domain name, and then the check runs. The
   * check gets the new domain with its backing tag set, the parent with its own parent set, no
   * sub-domains, the tag, and the domains the tag already backs.
   *
   * <p>This operation must own its transaction so a failed check rolls back the inserted domain. It
   * cannot run inside another transaction.
   *
   * @param domainEntity The domain to insert; its parent, if set, is the parent to insert it under.
   * @param backingTag The existing tag, with an optional value, that backs the domain.
   * @param check The check of the locked state; nothing is inserted if it throws.
   * @throws IOException If the domain cannot be stored.
   * @throws EntityAlreadyExistsException If a domain with the same name exists.
   * @throws NoSuchEntityException If the metalake or the parent does not exist.
   * @throws IllegalStateException If it is called inside a transaction.
   * @throws NoSuchTagException If the tag does not exist.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertDomainWithBackingTag")
  public void insertDomainWithBackingTag(
      DomainEntity domainEntity, TagValue backingTag, Consumer<LockedDomainState> check)
      throws IOException {
    insertBackedDomain(
        domainEntity,
        backingTag.value().orElse(null),
        metalakePO -> {
          Namespace tagNamespace = NamespaceUtil.ofTag(metalakePO.getMetalakeName());
          TagEntity tag =
              TagMetaService.getInstance()
                  .lockAndGetTagForDomainBinding(
                      metalakePO.getMetalakeId(), tagNamespace, backingTag.name());
          return Pair.of(
              tag,
              listDomainsByBackingTagId(
                  metalakePO.getMetalakeId(), tag.id(), domainEntity.namespace()));
        },
        check,
        null);
  }

  /**
   * Inserts a new domain together with a new tag that backs it without a value, after a check of
   * the locked state. If the domain has a parent, it is inserted as a sub-domain of that parent,
   * together with its parent edge. The tag, the domain and the edge are written in one transaction.
   *
   * <p>The metalake row is locked shared, then the parent row, if any, shared. The new tag has no
   * row to lock; the unique key on the tag name fails the insert if a tag with the same name exists
   * or is created at the same time. The check gets the new domain with its backing tag set, the
   * parent with its own parent set, no sub-domains, the new tag, and no other domains.
   *
   * @param domainEntity The domain to insert; its parent, if set, is the parent to insert it under.
   * @param backingTag The new tag that backs the domain.
   * @param check The check of the locked state; nothing is inserted if it throws.
   * @throws IOException If the domain cannot be stored.
   * @throws EntityAlreadyExistsException If a domain with the same name exists.
   * @throws NoSuchEntityException If the metalake or the parent does not exist.
   * @throws TagAlreadyExistsException If a tag with the same name exists.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertDomainWithNewBackingTag")
  public void insertDomainWithNewBackingTag(
      DomainEntity domainEntity, TagEntity backingTag, Consumer<LockedDomainState> check)
      throws IOException {
    insertBackedDomain(
        domainEntity,
        null,
        metalakePO -> Pair.of(backingTag, Collections.emptyList()),
        check,
        backingTag);
  }

  /**
   * Sets or removes the backing tag of a domain, after a check of the locked state. Setting the
   * current backing tag writes nothing.
   *
   * <p>The metalake row is locked shared, then the domain row exclusively, then the new tag, if
   * any, exclusively, so the tag cannot be deleted or bound by another domain until the change
   * commits. The check gets the domain with its current parent and backing tag set, no parent and
   * no sub-domains, the new tag, and the domains the tag already backs, which include this domain
   * if the tag already backs it.
   *
   * @param identifier The identifier of the domain.
   * @param backingTag The existing tag, with an optional value, that backs the domain, or null to
   *     remove the backing tag.
   * @param auditInfo The audit info of the change; the domain keeps its creator and takes its last
   *     modifier.
   * @param check The check of the locked state; nothing is written if it throws.
   * @return The domain after the change.
   * @throws IOException If the domain cannot be stored.
   * @throws NoSuchEntityException If the domain does not exist.
   * @throws NoSuchTagException If the tag does not exist.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateDomainBackingTagAndGet")
  public DomainEntity updateDomainBackingTagAndGet(
      NameIdentifier identifier,
      @Nullable TagValue backingTag,
      AuditInfo auditInfo,
      Consumer<LockedDomainState> check)
      throws IOException {
    Namespace namespace = identifier.namespace();
    String metalakeName = namespace.level(0);
    DomainEntity[] updated = new DomainEntity[1];
    try {
      DomainPO observedPO = getDomainPOByMetalakeAndName(metalakeName, identifier.name());
      SessionUtils.doMultipleWithCommit(
          () -> {
            DomainPO lockedPO =
                DomainRelService.getInstance().lockAndGetDomainForChange(identifier, observedPO);
            DomainEntity domain =
                DomainRelService.getInstance().toEntityWithParent(lockedPO, namespace);
            TagEntity tag = null;
            List<DomainEntity> bindings = Collections.emptyList();
            if (backingTag != null) {
              tag =
                  TagMetaService.getInstance()
                      .lockAndGetTagForDomainBinding(
                          lockedPO.metalakeId(),
                          NamespaceUtil.ofTag(metalakeName),
                          backingTag.name());
              bindings = listDomainsByBackingTagId(lockedPO.metalakeId(), tag.id(), namespace);
            }
            check.accept(
                new LockedDomainState(domain, null, Collections.emptyList(), tag, bindings));

            Long newTagId = tag == null ? null : tag.id();
            String newValue = backingTag == null ? null : backingTag.value().orElse(null);
            if (!Objects.equals(lockedPO.backingTagId(), newTagId)
                || !Objects.equals(lockedPO.backingTagValue(), newValue)) {
              DomainEntity newDomain =
                  domain.toBuilder()
                      .withBackingTagId(newTagId)
                      .withBackingTagName(tag == null ? null : tag.name())
                      .withBackingTagValue(newValue)
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
            updated[0] =
                fromDomainPOWithParent(
                    SessionUtils.getWithoutCommit(
                        DomainMetaMapper.class,
                        mapper -> mapper.selectDomainMetaById(lockedPO.domainId())),
                    namespace);
          });
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.DOMAIN, identifier.toString());
      throw e;
    }
    return updated[0];
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
   * <p>The returned domain is read back in the same transaction, so its backing tag name is the
   * tag's current name rather than the one the updater's entity carries. The parent, the status and
   * the backing tag cannot be changed by an update, an updater that changes any of them is
   * rejected; {@link DomainRelService} and {@link #updateDomainBackingTagAndGet} change them under
   * the locks their rules need.
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
      DomainEntity[] storedDomain = new DomainEntity[1];
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
            Preconditions.checkArgument(
                oldDomainEntity.status() == updatedDomainEntity.status(),
                "The status of domain %s cannot be changed by an update, use"
                    + " DomainRelService#updateDomainStatusAndGet",
                identifier);
            Preconditions.checkArgument(
                sameBackingTag(oldDomainEntity, updatedDomainEntity),
                "The backing tag of domain %s cannot be changed by an update, use"
                    + " updateDomainBackingTagAndGet",
                identifier);

            DomainPO newDomainPO = DomainPO.updateDomainPO(lockedDomainPO, updatedDomainEntity);
            updateDomainWithVersion(identifier, mapper, lockedDomainPO, newDomainPO);
            storedDomain[0] =
                DomainPO.fromDomainPO(
                    mapper.selectDomainMetaById(lockedDomainPO.domainId()), identifier.namespace());
          });

      return storedDomain[0];
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
          () -> {
            DomainPO lockedDomainPO = lockAndGetDomainPO(identifier, domainPO);
            DomainRelService.getInstance().checkDomainHasNoSubDomains(identifier, domainId);
            deleteDomainWithVersion(identifier, lockedDomainPO);
          },
          () ->
              SessionUtils.doWithoutCommit(
                  DomainRelMapper.class, mapper -> mapper.softDeleteDomainRelByChildId(domainId)),
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
   * Inserts a new domain, and a new backing tag if one is given, in one transaction: locks the
   * metalake shared, then the parent, if any, shared, then gets the backing tag and its bindings
   * from {@code backingTagLocker}, writes the domain and the new tag, runs the check, and writes
   * the parent edge.
   */
  private void insertBackedDomain(
      DomainEntity domainEntity,
      @Nullable String backingTagValue,
      Function<MetalakePO, Pair<TagEntity, List<DomainEntity>>> backingTagLocker,
      Consumer<LockedDomainState> check,
      @Nullable TagEntity newBackingTag)
      throws IOException {
    Preconditions.checkState(
        !SessionUtils.isInTransaction(),
        "The domain %s cannot be inserted with a check inside another transaction",
        domainEntity.nameIdentifier());
    Namespace ns = domainEntity.namespace();
    String metalakeName = ns.level(0);

    try {
      MetalakePO metalakePO = getMetalakePO(metalakeName);
      SessionUtils.doMultipleWithCommit(
          () ->
              lockMetalakeForDomainWrite(metalakePO.getMetalakeName(), metalakePO.getMetalakeId()),
          () -> {
            DomainEntity parent =
                domainEntity
                    .parent()
                    .map(
                        parentName ->
                            DomainRelService.getInstance()
                                .lockAndGetParentOfNewSubDomain(NameIdentifier.of(ns, parentName)))
                    .orElse(null);
            Pair<TagEntity, List<DomainEntity>> backingTag = backingTagLocker.apply(metalakePO);
            DomainEntity domain =
                domainEntity.toBuilder()
                    .withBackingTagId(backingTag.getLeft().id())
                    .withBackingTagName(backingTag.getLeft().name())
                    .withBackingTagValue(backingTagValue)
                    .build();
            DomainPO domainPO = DomainPO.initializeDomainPO(domain, metalakePO.getMetalakeId());
            // The domain is written before the check, so that a duplicate name fails as such.
            insertNewDomainWithoutCommit(domainPO);
            if (newBackingTag != null) {
              TagMetaService.getInstance()
                  .insertTagOfNewDomain(newBackingTag, metalakePO.getMetalakeId());
            }
            check.accept(
                new LockedDomainState(
                    domain,
                    parent,
                    Collections.emptyList(),
                    backingTag.getLeft(),
                    backingTag.getRight()));
            if (parent != null) {
              DomainRelService.getInstance()
                  .insertDomainRelOfNewSubDomain(
                      metalakePO.getMetalakeId(),
                      parent.id(),
                      domainPO.domainId(),
                      AuditInfo.builder()
                          .withCreator(domain.auditInfo().creator())
                          .withCreateTime(domain.auditInfo().createTime())
                          .build());
            }
          });
    } catch (TagAlreadyExistsException e) {
      // Its cause is the duplicate key of the tag, which must not be reported as the domain's.
      throw e;
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(
          e, Entity.EntityType.DOMAIN, domainEntity.nameIdentifier().toString());
      throw e;
    }
  }

  // Whether two domains are bound to the same backing tag ID and value.
  private static boolean sameBackingTag(DomainEntity domain, DomainEntity other) {
    return Objects.equals(domain.backingTagId(), other.backingTagId())
        && Objects.equals(domain.backingTagValue(), other.backingTagValue());
  }

  // Lists the domains backed by a tag, read in the caller's transaction under the lock on the tag.
  private List<DomainEntity> listDomainsByBackingTagId(
      Long metalakeId, Long backingTagId, Namespace namespace) {
    return SessionUtils.getWithoutCommit(
            DomainMetaMapper.class,
            mapper -> mapper.listDomainPOsByBackingTagId(metalakeId, backingTagId))
        .stream()
        .map(po -> DomainPO.fromDomainPO(po, namespace))
        .collect(Collectors.toList());
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

    Preconditions.checkArgument(
        Objects.equals(existingDomainPO.status(), domainEntity.status().name()),
        "The status of domain %s cannot be changed by replacing it",
        domainEntity.nameIdentifier());
    Preconditions.checkArgument(
        Objects.equals(existingDomainPO.backingTagId(), domainEntity.backingTagId())
            && Objects.equals(existingDomainPO.backingTagValue(), domainEntity.backingTagValue()),
        "The backing tag of domain %s cannot be changed by replacing it",
        domainEntity.nameIdentifier());
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

  private MetalakePO getMetalakePO(String metalakeName) {
    MetalakePO metalakePO =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(metalakeName));
    if (metalakePO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.METALAKE.name().toLowerCase(),
          metalakeName);
    }
    return metalakePO;
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
    return DomainPO.fromDomainPO(domainPO, namespace);
  }

  private List<DomainEntity> fromDomainPOsWithParents(
      List<DomainPO> domainPOs, Namespace namespace) {
    return domainPOs.stream()
        .map(po -> DomainPO.fromDomainPO(po, namespace))
        .collect(Collectors.toList());
  }
}
