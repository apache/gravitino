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
package org.apache.gravitino.domain;

import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.DomainAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyDomainException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.DomainEntity;

/**
 * The domain operations of the server. Domains are one level deep: a domain is either top-level or
 * a sub-domain of one top-level domain. Creating a sub-domain, attaching a domain and moving it
 * check that:
 *
 * <ul>
 *   <li>a domain is not its own parent;
 *   <li>the parent is top-level, so a sub-domain cannot have sub-domains;
 *   <li>the attached domain has no sub-domains, so it does not bring a second level with it;
 *   <li>parent and child are in the same metalake;
 *   <li>a domain has at most one parent, so attaching a sub-domain moves it;
 *   <li>a published domain is attached only under a published parent.
 * </ul>
 *
 * <p>The first three keep the hierarchy one level deep, so it cannot have a cycle.
 *
 * <p>A new domain is a draft. A sub-domain can be published only under a published parent, and a
 * domain can be unpublished only when none of its sub-domains is published.
 *
 * <p>The rules are checked against the domains as they are under the row locks of the change, so a
 * concurrent change cannot break them. A rule that does not hold fails the operation with an {@link
 * IllegalArgumentException}.
 */
@Evolving
public interface DomainDispatcher {

  /**
   * Lists the names of the domains in a metalake, including sub-domains.
   *
   * @param metalake The name of the metalake.
   * @return The names of the domains.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  default String[] listDomains(String metalake) throws NoSuchMetalakeException {
    return Arrays.stream(listDomainInfos(metalake)).map(DomainEntity::name).toArray(String[]::new);
  }

  /**
   * Lists the domains in a metalake, including sub-domains.
   *
   * @param metalake The name of the metalake.
   * @return The domains.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  DomainEntity[] listDomainInfos(String metalake) throws NoSuchMetalakeException;

  /**
   * Gets a domain.
   *
   * @param metalake The name of the metalake.
   * @param name The name of the domain.
   * @return The domain.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws NoSuchDomainException If the domain does not exist.
   */
  DomainEntity getDomain(String metalake, String name) throws NoSuchDomainException;

  // TODO: The next PR of #13884 adds the explicit backing tag to createDomain and
  // createSubdomain, which every create request must name, and adds updateBackingTag.

  /**
   * Creates a top-level domain in draft status.
   *
   * @param metalake The name of the metalake.
   * @param name The name of the domain.
   * @param comment The comment of the domain, or null.
   * @param properties The properties of the domain, or null.
   * @return The created domain.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws DomainAlreadyExistsException If a domain with the same name exists.
   */
  DomainEntity createDomain(
      String metalake,
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties)
      throws DomainAlreadyExistsException;

  /**
   * Creates a sub-domain in draft status under a top-level domain.
   *
   * @param metalake The name of the metalake.
   * @param parent The name of the parent domain.
   * @param name The name of the sub-domain.
   * @param comment The comment of the sub-domain, or null.
   * @param properties The properties of the sub-domain, or null.
   * @return The created sub-domain.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws NoSuchDomainException If the parent does not exist.
   * @throws DomainAlreadyExistsException If a domain with the same name exists.
   * @throws IllegalArgumentException If the parent is the sub-domain itself or a sub-domain.
   */
  DomainEntity createSubdomain(
      String metalake,
      String parent,
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties)
      throws NoSuchDomainException, DomainAlreadyExistsException, IllegalArgumentException;

  /**
   * Alters a domain. Its status and parent are not changed by this method.
   *
   * @param metalake The name of the metalake.
   * @param name The name of the domain.
   * @param changes The changes to apply.
   * @return The altered domain.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws NoSuchDomainException If the domain does not exist.
   * @throws DomainAlreadyExistsException If the domain is renamed to a name in use.
   * @throws IllegalArgumentException If a change cannot be applied.
   */
  DomainEntity alterDomain(String metalake, String name, DomainChange... changes)
      throws NoSuchDomainException, DomainAlreadyExistsException, IllegalArgumentException;

  /**
   * Deletes a domain, with its parent edge.
   *
   * @param metalake The name of the metalake.
   * @param name The name of the domain.
   * @return True if the domain is deleted, false if it does not exist.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws NonEmptyDomainException If the domain has sub-domains.
   */
  boolean deleteDomain(String metalake, String name) throws NonEmptyDomainException;

  /**
   * Publishes a domain. Publishing a published domain changes nothing.
   *
   * @param metalake The name of the metalake.
   * @param name The name of the domain.
   * @return The published domain.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws NoSuchDomainException If the domain does not exist.
   * @throws IllegalArgumentException If the domain is a sub-domain of a draft parent.
   * @throws OptimisticLockException If the parent keeps changing while the status is changed.
   */
  DomainEntity publishDomain(String metalake, String name)
      throws NoSuchDomainException, IllegalArgumentException;

  /**
   * Unpublishes a domain. Unpublishing a draft domain changes nothing.
   *
   * @param metalake The name of the metalake.
   * @param name The name of the domain.
   * @return The unpublished domain.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws NoSuchDomainException If the domain does not exist.
   * @throws IllegalArgumentException If the domain has published sub-domains.
   * @throws OptimisticLockException If the parent keeps changing while the status is changed.
   */
  DomainEntity unpublishDomain(String metalake, String name)
      throws NoSuchDomainException, IllegalArgumentException;

  /**
   * Lists the names of the sub-domains of a domain.
   *
   * @param metalake The name of the metalake.
   * @param name The name of the domain.
   * @return The names of the sub-domains.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws NoSuchDomainException If the domain does not exist.
   */
  default String[] listSubdomains(String metalake, String name) throws NoSuchDomainException {
    return Arrays.stream(listSubdomainInfos(metalake, name))
        .map(DomainEntity::name)
        .toArray(String[]::new);
  }

  /**
   * Lists the sub-domains of a domain.
   *
   * @param metalake The name of the metalake.
   * @param name The name of the domain.
   * @return The sub-domains.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws NoSuchDomainException If the domain does not exist.
   */
  DomainEntity[] listSubdomainInfos(String metalake, String name) throws NoSuchDomainException;

  /**
   * Attaches a domain under a parent, or moves a sub-domain to another parent. Attaching a domain
   * to its current parent changes nothing.
   *
   * @param metalake The name of the metalake.
   * @param name The name of the domain.
   * @param parent The name of the new parent.
   * @return The attached domain.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws NoSuchDomainException If the domain or the parent does not exist.
   * @throws IllegalArgumentException If the domain cannot be attached under the parent.
   */
  DomainEntity attachDomain(String metalake, String name, String parent)
      throws NoSuchDomainException, IllegalArgumentException;

  /**
   * Detaches a domain from its current parent, making it top-level. Detaching a top-level domain
   * changes nothing.
   *
   * @param metalake The name of the metalake.
   * @param name The name of the domain.
   * @return The detached domain.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws NoSuchDomainException If the domain does not exist.
   */
  DomainEntity detachDomain(String metalake, String name) throws NoSuchDomainException;
}
