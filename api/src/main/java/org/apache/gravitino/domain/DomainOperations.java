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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.BackingTagAlreadyBoundException;
import org.apache.gravitino.exceptions.DomainAlreadyExistsException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.NonEmptyDomainException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.tag.TagValue;

/**
 * Interface for the domain operations of a metalake. Operations on a single domain, such as
 * publishing it or managing its sub-domains, are on {@link Domain}.
 *
 * <p>A domain in {@link Domain.Status#DRAFT} status is returned only to its owner and to the users
 * who can manage it, its parent or the metalake. For other users it is omitted from lists, and
 * getting it throws {@link NoSuchDomainException}.
 */
@Evolving
public interface DomainOperations {

  /**
   * Lists the names of the domains in the metalake, including sub-domains.
   *
   * @return The names of the domains.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  String[] listDomains() throws NoSuchMetalakeException;

  /**
   * Lists the domains in the metalake with details, including sub-domains.
   *
   * @return The domains.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  Domain[] listDomainsInfo() throws NoSuchMetalakeException;

  /**
   * Gets a domain by its name.
   *
   * @param name The name of the domain.
   * @return The domain.
   * @throws NoSuchDomainException If the domain does not exist or is not visible to the caller.
   */
  Domain getDomain(String name) throws NoSuchDomainException;

  /**
   * Creates a top-level domain in {@link Domain.Status#DRAFT} status. Use {@link
   * Domain#createSubdomain(String, String, Map, TagValue, boolean)} to create a sub-domain.
   *
   * <p>The backing tag is always given explicitly. With {@code createBackingTag} set, a new tag
   * without values is created with the given name and owned by the caller; otherwise the given
   * existing tag, with its optional value, is bound. An existing tag is never adopted implicitly.
   *
   * @param name The name of the domain.
   * @param comment The comment of the domain, or null.
   * @param properties The properties of the domain, or null.
   * @param backingTag The backing tag of the domain: the name of the tag to create, or an existing
   *     tag with an optional value.
   * @param createBackingTag Whether to create the backing tag as a new tag.
   * @return The created domain.
   * @throws DomainAlreadyExistsException If a domain with the same name already exists.
   * @throws NoSuchTagException If the existing backing tag to bind does not exist.
   * @throws TagAlreadyExistsException If the backing tag to create already exists.
   * @throws BackingTagAlreadyBoundException If the binding conflicts with the backing tag of
   *     another domain.
   * @throws ForbiddenException If the caller does not own the existing backing tag or the metalake,
   *     or may not create tags.
   * @throws IllegalArgumentException If the backing tag is null, a value is given for a tag to
   *     create, or the value is not allowed by the existing tag.
   */
  Domain createDomain(
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      TagValue backingTag,
      boolean createBackingTag)
      throws DomainAlreadyExistsException, NoSuchTagException, TagAlreadyExistsException,
          BackingTagAlreadyBoundException, ForbiddenException, IllegalArgumentException;

  /**
   * Alters a domain. Renaming a domain does not rename its backing tag.
   *
   * @param name The name of the domain.
   * @param changes The changes to apply to the domain.
   * @return The altered domain.
   * @throws NoSuchDomainException If the domain does not exist or is not visible to the caller.
   * @throws DomainAlreadyExistsException If the domain is renamed and a domain with the new name
   *     already exists.
   * @throws IllegalArgumentException If the changes cannot be applied to the domain.
   */
  Domain alterDomain(String name, DomainChange... changes)
      throws NoSuchDomainException, DomainAlreadyExistsException, IllegalArgumentException;

  /**
   * Deletes a domain. The backing tag and the tags on the member objects are kept.
   *
   * @param name The name of the domain.
   * @return True if the domain is deleted, false if it does not exist.
   * @throws NonEmptyDomainException If the domain has sub-domains.
   */
  boolean deleteDomain(String name) throws NonEmptyDomainException;
}
