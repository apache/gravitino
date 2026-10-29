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
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;

/**
 * The domain changes whose rules depend on other domains: creating a domain, changing its parent
 * and changing its status. Each change runs in one transaction. It locks the rows of the domains
 * involved, reads them, hands them to the caller's check as a {@link LockedDomainState}, and writes
 * the change only if the check returns. A check that throws rolls the change back, and the
 * exception reaches the caller unchanged.
 *
 * <p>The store only locks, reads and writes; the rules themselves, such as how deep the hierarchy
 * may be or which status a parent needs, are the caller's check. Since the check sees the domains
 * as they are under the locks, a rule it enforces cannot be broken by a concurrent change that the
 * locks exclude.
 *
 * <p>In a {@link LockedDomainState}, the domain being changed and its parent are read with locking
 * reads, which do not read the backing tag name, so their {@link DomainEntity#backingTag()} is
 * empty; their backing tag ID and value are set. For an insert, the domain is the caller's new
 * entity.
 */
public interface SupportsDomainOperations {

  /**
   * Inserts a domain. If the domain has a parent, it is inserted as a sub-domain of that parent,
   * together with its parent edge.
   *
   * <p>The metalake is locked shared and the parent, if any, is locked shared, so the parent cannot
   * be attached elsewhere, deleted or unpublished before the change commits. The check gets the new
   * domain, the parent with its own parent set, and no sub-domains.
   *
   * @param domain The domain to insert.
   * @param check The check of the locked state.
   * @throws IOException If the domain cannot be stored.
   * @throws EntityAlreadyExistsException If a domain with the same name exists.
   * @throws NoSuchEntityException If the metalake or the parent does not exist.
   */
  void insertDomain(DomainEntity domain, Consumer<LockedDomainState> check)
      throws IOException, EntityAlreadyExistsException, NoSuchEntityException;

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
  DomainEntity setDomainParent(
      NameIdentifier ident,
      NameIdentifier parentIdent,
      AuditInfo auditInfo,
      Consumer<LockedDomainState> check)
      throws IOException, NoSuchEntityException;

  /**
   * Removes the parent edge of a domain, which makes it top-level. Removing the parent of a
   * top-level domain changes nothing.
   *
   * @param ident The identifier of the domain.
   * @return The domain after the change.
   * @throws IOException If the change cannot be stored.
   * @throws NoSuchEntityException If the domain does not exist.
   */
  DomainEntity removeDomainParent(NameIdentifier ident) throws IOException, NoSuchEntityException;

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
  DomainEntity updateDomainStatus(
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

    /**
     * Creates the locked state of a change.
     *
     * @param domain The domain being changed, with its current parent set.
     * @param parent The parent involved in the change, with its own parent set, or null if none.
     * @param subDomains The sub-domains of the domain being changed.
     */
    public LockedDomainState(
        DomainEntity domain, @Nullable DomainEntity parent, List<DomainEntity> subDomains) {
      this.domain = domain;
      this.parent = parent;
      this.subDomains = ImmutableList.copyOf(subDomains);
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
     * changes.
     *
     * @return The parent, or empty if the change involves none.
     */
    public Optional<DomainEntity> parent() {
      return Optional.ofNullable(parent);
    }

    /**
     * Returns the sub-domains of the domain being changed. For an insert, there are none.
     *
     * @return The sub-domains.
     */
    public List<DomainEntity> subDomains() {
      return subDomains;
    }
  }
}
