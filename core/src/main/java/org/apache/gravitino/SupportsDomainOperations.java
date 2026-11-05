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
package org.apache.gravitino;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.tag.TagValue;

/**
 * The domain changes whose rules depend on other domains, or whose authorization depends on the
 * current parent: creating a domain, changing or removing its parent, changing its status and
 * changing its backing tag. Each change runs in one transaction. It locks the rows of the domains
 * involved, reads them, hands them to the caller's check as a {@link LockedDomainState}, and writes
 * the change only if the check returns. A check that throws rolls the change back, and the
 * exception reaches the caller unchanged, unless it is caused by a database error, which is
 * reported like any other failure of the store.
 *
 * <p>The store only locks, reads and writes; the rules themselves, such as how deep the hierarchy
 * may be or which status a parent needs, are the caller's check. Since the check sees the domains
 * as they are under the locks, a rule it enforces cannot be broken by a concurrent change that the
 * locks exclude.
 *
 * <p>In a {@link LockedDomainState}, the domain being changed and its parent are read with locking
 * reads, which do not read the backing tag name, so their {@link DomainEntity#backingTag()} is
 * empty; their backing tag ID and value are set. For an insert, the domain is the caller's new
 * entity with its backing tag set.
 */
public interface SupportsDomainOperations {

  /**
   * Inserts a domain backed by an existing tag. If the domain has a parent, it is inserted as a
   * sub-domain of that parent, together with its parent edge.
   *
   * <p>The metalake is locked shared, the parent, if any, shared, and the tag exclusively, so the
   * parent cannot be attached elsewhere, deleted or unpublished, and the tag cannot be deleted or
   * bound to another domain, before the change commits. The tag is resolved by name and re-checked
   * after it is locked. The parent and the tag are locked before the domain is written, so a
   * missing parent or tag is reported before a duplicate domain name; the domain is written before
   * the check runs, so a duplicate name is reported before a broken rule. The check gets the new
   * domain with its backing tag set, the parent with its own parent set, no sub-domains, the tag,
   * and the domains it already backs. This operation must own its transaction and cannot run inside
   * another transaction.
   *
   * @param domain The domain to insert, without a backing tag.
   * @param backingTag The existing tag, with an optional value, that backs the domain.
   * @param check The check of the locked state.
   * @throws IOException If the domain cannot be stored.
   * @throws EntityAlreadyExistsException If a domain with the same name exists.
   * @throws NoSuchEntityException If the metalake or the parent does not exist.
   * @throws NoSuchTagException If the tag does not exist.
   * @throws IllegalStateException If it is called inside a transaction.
   */
  void insertDomainWithBackingTag(
      DomainEntity domain, TagValue backingTag, Consumer<LockedDomainState> check)
      throws IOException, EntityAlreadyExistsException, NoSuchEntityException, NoSuchTagException;

  /**
   * Inserts a domain together with a new tag that backs it without a value. If the domain has a
   * parent, it is inserted as a sub-domain of that parent, together with its parent edge. The tag,
   * the domain and the edge are written in one transaction, so a failure leaves none of them.
   *
   * <p>The metalake is locked shared and the parent, if any, shared. The new tag has no row to lock
   * yet; the unique key on its name fails a concurrent tag with the same name. The domain is
   * written before the tag, so a duplicate domain name is reported before a duplicate tag name, and
   * both are written before the check runs, so a duplicate name is reported before a broken rule.
   * The check gets the new domain with its backing tag set, the parent with its own parent set, no
   * sub-domains, the new tag, and no other domains.
   *
   * @param domain The domain to insert, without a backing tag.
   * @param backingTag The new tag that backs the domain.
   * @param check The check of the locked state.
   * @throws IOException If the domain cannot be stored.
   * @throws EntityAlreadyExistsException If a domain with the same name exists.
   * @throws NoSuchEntityException If the metalake or the parent does not exist.
   * @throws TagAlreadyExistsException If a tag with the same name exists.
   * @throws IllegalStateException If it is called inside a transaction.
   */
  void insertDomainWithNewBackingTag(
      DomainEntity domain, TagEntity backingTag, Consumer<LockedDomainState> check)
      throws IOException, EntityAlreadyExistsException, NoSuchEntityException,
          TagAlreadyExistsException;

  /**
   * Sets or removes the backing tag of a domain. Setting the current backing tag writes nothing.
   *
   * <p>The metalake is locked shared, the domain exclusively and then the new tag, if any,
   * exclusively, so the tag cannot be deleted or bound to another domain before the change commits.
   * The tag is resolved by name and re-checked after it is locked. The check gets the domain with
   * its current parent and backing tag set, no parent and no sub-domains, the new tag, and the
   * domains it already backs, which include this domain if it is already backed by the tag.
   *
   * @param ident The identifier of the domain.
   * @param backingTag The existing tag, with an optional value, that backs the domain, or null to
   *     remove the backing tag.
   * @param auditInfo The audit info of the change; the domain keeps its creator and takes its last
   *     modifier.
   * @param check The check of the locked state.
   * @return The domain after the change.
   * @throws IOException If the change cannot be stored.
   * @throws NoSuchEntityException If the domain does not exist.
   * @throws NoSuchTagException If the tag does not exist.
   */
  DomainEntity updateDomainBackingTagAndGet(
      NameIdentifier ident,
      @Nullable TagValue backingTag,
      AuditInfo auditInfo,
      Consumer<LockedDomainState> check)
      throws IOException, NoSuchEntityException, NoSuchTagException;

  /**
   * Sets the parent of a domain: attaches a top-level domain, or moves a sub-domain to another
   * parent. Setting the current parent changes nothing.
   *
   * <p>The domain is locked exclusively and the new parent shared, in domain ID order, so before
   * the change commits neither one's parent can change, the new parent's status cannot change, and
   * no sub-domain can be added to the domain. The check gets the domain with its current parent
   * set, the new parent with its own parent set, and the sub-domains of the domain.
   *
   * @param ident The identifier of the domain.
   * @param parentIdent The identifier of the new parent, in the same metalake.
   * @param auditInfo The audit info of the change. A new parent edge is created by its creator; a
   *     moved edge keeps its creator and takes its last modifier.
   * @param check The check of the locked state.
   * @return The domain after the change.
   * @throws IOException If the change cannot be stored.
   * @throws NoSuchEntityException If the domain or the new parent does not exist.
   * @throws IllegalArgumentException If the new parent is the domain itself or is in another
   *     metalake.
   */
  DomainEntity setDomainParentAndGet(
      NameIdentifier ident,
      NameIdentifier parentIdent,
      AuditInfo auditInfo,
      Consumer<LockedDomainState> check)
      throws IOException, NoSuchEntityException;

  /**
   * Removes the parent edge of a domain, which makes it top-level. Removing the parent of a
   * top-level domain changes nothing.
   *
   * <p>The domain is locked exclusively, so its parent cannot change before the change commits. The
   * check gets the domain with its current parent set, which is the parent the edge is removed
   * from; the parent itself is not locked, so the state has no parent entity and no sub-domains.
   *
   * @param ident The identifier of the domain.
   * @param check The check of the locked state.
   * @return The domain after the change.
   * @throws IOException If the change cannot be stored.
   * @throws NoSuchEntityException If the domain does not exist.
   */
  DomainEntity removeDomainParentAndGet(NameIdentifier ident, Consumer<LockedDomainState> check)
      throws IOException, NoSuchEntityException;

  /**
   * Sets the status of a domain. Setting the current status writes nothing.
   *
   * <p>The domain is locked exclusively and its current parent shared, in domain ID order, so
   * before the change commits the parent's status and the domain's parent cannot change, and no
   * sub-domain can be added to the domain or change its status; a sub-domain can still be detached
   * or deleted. The parent is read before the locks are taken; if it changed by the time they are
   * held, the change is rolled back and retried with the new parent, a bounded number of times, so
   * it cannot run inside another transaction. The check gets the domain with its current parent
   * set, that parent, and the sub-domains of the domain.
   *
   * @param ident The identifier of the domain.
   * @param status The new status.
   * @param auditInfo The audit info of the change; the domain keeps its creator and takes its last
   *     modifier.
   * @param check The check of the locked state.
   * @return The domain after the change.
   * @throws IOException If the change cannot be stored.
   * @throws NoSuchEntityException If the domain does not exist.
   * @throws OptimisticLockException If the parent kept changing until the retries ran out.
   * @throws IllegalStateException If it is called inside a transaction.
   */
  DomainEntity updateDomainStatusAndGet(
      NameIdentifier ident,
      Domain.Status status,
      AuditInfo auditInfo,
      Consumer<LockedDomainState> check)
      throws IOException, NoSuchEntityException;

  /**
   * Lists the sub-domains of a domain.
   *
   * @param parentIdent The identifier of the parent domain.
   * @return The sub-domains, each with its parent set.
   * @throws IOException If the sub-domains cannot be read.
   * @throws NoSuchEntityException If the parent domain does not exist.
   */
  List<DomainEntity> listSubDomains(NameIdentifier parentIdent)
      throws IOException, NoSuchEntityException;

  /** The domains involved in a change, as read under the change's row locks. */
  final class LockedDomainState {

    private final DomainEntity domain;
    @Nullable private final DomainEntity parent;
    private final List<DomainEntity> subDomains;
    @Nullable private final TagEntity backingTag;
    private final List<DomainEntity> backingTagBindings;

    /**
     * Creates the locked state of a change that involves no backing tag.
     *
     * @param domain The domain being changed, with its current parent set.
     * @param parent The parent involved in the change, with its own parent set, or null if none.
     * @param subDomains The sub-domains of the domain being changed.
     */
    public LockedDomainState(
        DomainEntity domain, @Nullable DomainEntity parent, List<DomainEntity> subDomains) {
      this(domain, parent, subDomains, null, Collections.emptyList());
    }

    /**
     * Creates the locked state of a change.
     *
     * @param domain The domain being changed, with its current parent set.
     * @param parent The parent involved in the change, with its own parent set, or null if none.
     * @param subDomains The sub-domains of the domain being changed.
     * @param backingTag The backing tag the change binds, or null if none.
     * @param backingTagBindings The domains the backing tag backs before the change.
     */
    public LockedDomainState(
        DomainEntity domain,
        @Nullable DomainEntity parent,
        List<DomainEntity> subDomains,
        @Nullable TagEntity backingTag,
        List<DomainEntity> backingTagBindings) {
      this.domain = domain;
      this.parent = parent;
      this.subDomains = ImmutableList.copyOf(subDomains);
      this.backingTag = backingTag;
      this.backingTagBindings = ImmutableList.copyOf(backingTagBindings);
    }

    /**
     * Returns the domain being changed, with its current parent set. For an insert, it is the new
     * domain.
     *
     * @return The domain being changed.
     */
    public DomainEntity domain() {
      return domain;
    }

    /**
     * Returns the parent involved in the change, with its own parent set: the parent of a new
     * sub-domain, the new parent when the parent is set, or the current parent when the status
     * changes. It is empty when the parent is removed, since that parent is not locked.
     *
     * @return The parent, or empty if the change involves none.
     */
    public Optional<DomainEntity> parent() {
      return Optional.ofNullable(parent);
    }

    /**
     * Returns the sub-domains of the domain being changed. For an insert, there are none, and when
     * the parent is removed they are not read.
     *
     * @return The sub-domains.
     */
    public List<DomainEntity> subDomains() {
      return subDomains;
    }

    /**
     * Returns the backing tag the change binds, as it is under the lock: the existing tag to bind,
     * or the new tag to create.
     *
     * @return The backing tag, or empty if the change binds none.
     */
    public Optional<TagEntity> backingTag() {
      return Optional.ofNullable(backingTag);
    }

    /**
     * Returns the domains that the backing tag of the change backs before the change, read under
     * the lock on the tag. Their parents are not set.
     *
     * @return The domains bound to the backing tag.
     */
    public List<DomainEntity> backingTagBindings() {
      return backingTagBindings;
    }
  }
}
