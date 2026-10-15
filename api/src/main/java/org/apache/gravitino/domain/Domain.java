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
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.DomainAlreadyExistsException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.tag.TagValue;

/**
 * A domain groups metadata objects from any catalog under a business name, such as "Finance". A
 * metadata object is a member of a domain when it carries the domain's backing tag, directly or
 * inherited from its catalog or schema. For the same tag, the assignment nearest to the object
 * wins: a table that carries {@code dept=hr} under a schema that carries {@code dept=finance} is a
 * member of the domain backed by {@code dept=hr} only.
 *
 * <p>A domain is either top-level or a sub-domain of exactly one top-level domain. A new domain is
 * in {@link Status#DRAFT} status and is visible only to its owner and the users who can manage it
 * until it is published.
 *
 * <p>A {@code Domain} is a snapshot. The operations on it that change the domain itself, such as
 * {@link #publish()} and {@link #attachToParent(String)}, return a new snapshot and leave this one
 * unchanged. They apply to the current state of the domain, not to the state this snapshot shows,
 * and the last write wins.
 */
@Evolving
public interface Domain extends Auditable {

  /** The status of a domain. */
  enum Status {
    /** The domain is visible only to its owner and the users who can manage it. */
    DRAFT,
    /** The domain is visible to all users who can use it. */
    PUBLISHED
  }

  /**
   * Returns the name of the domain.
   *
   * @return The name of the domain.
   */
  String name();

  /**
   * Returns the comment of the domain.
   *
   * @return The comment of the domain, or null if it is not set.
   */
  @Nullable
  String comment();

  /**
   * Returns the properties of the domain.
   *
   * @return The properties of the domain.
   */
  Map<String, String> properties();

  /**
   * Returns the status of the domain.
   *
   * @return The status of the domain.
   */
  Status status();

  /**
   * Returns the name of the parent domain. The name is returned even when the parent is not visible
   * to the caller, for example a draft parent; getting that parent still fails with {@link
   * NoSuchDomainException}.
   *
   * @return The name of the parent domain, or empty if the domain is top-level.
   */
  Optional<String> parent();

  /**
   * Returns the backing tag of the domain. The metadata objects whose effective assignment of this
   * tag matches it are the members of the domain. A backing tag without a value matches every
   * assignment of the tag, with or without values. A backing tag with a value matches the
   * assignments whose values contain it, compared exactly and case-sensitively.
   *
   * @return The backing tag of the domain, or empty if the backing tag has been removed or deleted
   *     and no new one has been set.
   */
  Optional<TagValue> backingTag();

  /**
   * Returns the metadata object the domain membership is inherited from. It is set only on the
   * domains returned by {@link SupportsDomains#listDomainsInfo()}, when the object belongs to the
   * domain through the backing tag of its catalog or schema.
   *
   * @return The catalog or schema the membership is inherited from, or empty if the object carries
   *     the backing tag itself or the domain is not listed for a metadata object.
   */
  Optional<MetadataObject> inheritedFrom();

  /**
   * Publishes the domain, making it visible to all users who can use it. A sub-domain can be
   * published only when its parent is published. Publishing a published domain has no effect.
   *
   * @return The published domain.
   * @throws NoSuchDomainException If the domain does not exist or is not visible to the caller.
   * @throws IllegalArgumentException If the domain is a sub-domain and its parent is not published.
   */
  Domain publish() throws NoSuchDomainException, IllegalArgumentException;

  /**
   * Unpublishes the domain, making it visible only to its owner and the users who can manage it. A
   * domain can be unpublished only when it has no published sub-domains. Unpublishing a draft
   * domain has no effect.
   *
   * @return The unpublished domain.
   * @throws NoSuchDomainException If the domain does not exist or is not visible to the caller.
   * @throws IllegalArgumentException If the domain has published sub-domains.
   */
  Domain unpublish() throws NoSuchDomainException, IllegalArgumentException;

  /**
   * Attaches this domain as a sub-domain of the given parent. If this domain is a sub-domain of
   * another domain, it is moved under the given parent. Attaching to the current parent has no
   * effect. The backing tag is not renamed.
   *
   * @param parent The name of the parent domain.
   * @return The attached domain.
   * @throws NoSuchDomainException If this domain or the parent does not exist or is not visible to
   *     the caller.
   * @throws IllegalArgumentException If this domain cannot be attached to the parent, for example
   *     because the parent is this domain or a sub-domain, this domain has sub-domains, or this
   *     domain is published and the parent is not.
   */
  Domain attachToParent(String parent) throws NoSuchDomainException, IllegalArgumentException;

  /**
   * Detaches this domain from its current parent, making it a top-level domain, whatever parent
   * this snapshot shows. Detaching a top-level domain has no effect. The backing tag is not
   * renamed.
   *
   * @return The detached domain.
   * @throws NoSuchDomainException If this domain does not exist or is not visible to the caller.
   */
  Domain detachFromParent() throws NoSuchDomainException;

  /**
   * Replaces the current backing tag of this domain, or removes it, whatever backing tag this
   * snapshot shows. Members are not migrated: objects that carry only the old backing tag are no
   * longer members of the domain. Setting the current backing tag has no effect.
   *
   * @param backingTag The existing tag, with an optional value, that backs the domain, or null to
   *     remove the backing tag.
   * @return The updated domain.
   * @throws NoSuchDomainException If this domain does not exist or is not visible to the caller.
   * @throws NoSuchTagException If the tag does not exist.
   * @throws ForbiddenException If the caller does not own the tag or the metalake.
   * @throws IllegalArgumentException If the value is not allowed by the tag.
   */
  Domain updateBackingTag(@Nullable TagValue backingTag)
      throws NoSuchDomainException, NoSuchTagException, ForbiddenException,
          IllegalArgumentException;

  /**
   * Creates a sub-domain under this domain. The new sub-domain is in {@link Status#DRAFT} status.
   *
   * <p>If no backing tag is given, a tag named after the domain path, such as "Finance/Trading", is
   * created and used as the backing tag. If a tag with that name already exists, it is used only
   * when it backs no other domain and the caller owns it or the metalake; otherwise the creation
   * fails.
   *
   * @param name The name of the sub-domain.
   * @param comment The comment of the sub-domain, or null.
   * @param properties The properties of the sub-domain, or null.
   * @param backingTag The existing tag, with an optional value, that backs the sub-domain, or null
   *     to create the default backing tag.
   * @return The created sub-domain.
   * @throws NoSuchDomainException If this domain does not exist or is not visible to the caller.
   * @throws DomainAlreadyExistsException If a domain with the same name already exists.
   * @throws NoSuchTagException If the given backing tag does not exist.
   * @throws ForbiddenException If the caller does not own the given backing tag or the metalake, or
   *     a tag with the default name exists and the caller does not own it or the metalake.
   * @throws IllegalArgumentException If a tag with the default name already backs another domain,
   *     the value of the given backing tag is not allowed by the tag, this domain is itself a
   *     sub-domain, or the sub-domain cannot be created under this domain.
   */
  Domain createSubdomain(
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      @Nullable TagValue backingTag)
      throws NoSuchDomainException, DomainAlreadyExistsException, NoSuchTagException,
          ForbiddenException, IllegalArgumentException;

  /**
   * Lists the names of the sub-domains of this domain.
   *
   * @return The names of the sub-domains.
   * @throws NoSuchDomainException If this domain does not exist or is not visible to the caller.
   */
  String[] listSubdomains() throws NoSuchDomainException;

  /**
   * Lists the sub-domains of this domain with details.
   *
   * @return The sub-domains.
   * @throws NoSuchDomainException If this domain does not exist or is not visible to the caller.
   */
  Domain[] listSubdomainsInfo() throws NoSuchDomainException;

  /**
   * Lists the members of this domain that match the filter. Only objects of the {@link
   * DomainMember#SUPPORTED_TYPES supported types} are members, and only the members the caller can
   * access are returned.
   *
   * <p>Only the objects that carry a backing tag themselves are listed. A catalog or schema that
   * carries one stands for the objects under it, which are not listed one by one. An object is
   * listed when it comes from any of the domains the filter selects, and it is listed once, see
   * {@link DomainMember#subdomain()}.
   *
   * @param filter The filter of the members. {@code DomainMemberFilter.builder().build()} lists all
   *     members of this domain and its sub-domains.
   * @return The members of this domain.
   * @throws NoSuchDomainException If this domain does not exist or is not visible to the caller.
   */
  DomainMember[] listMembers(DomainMemberFilter filter) throws NoSuchDomainException;
}
