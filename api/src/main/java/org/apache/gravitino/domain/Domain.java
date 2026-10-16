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
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.BackingTagAlreadyBoundException;
import org.apache.gravitino.exceptions.DomainAlreadyExistsException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.tag.TagValue;

/**
 * A domain groups metadata objects from any catalog under a business name, such as "Finance". A
 * leaf metadata object, of a type in {@link DomainMember#SUPPORTED_TYPES}, is a member of a domain
 * when it carries the domain's backing tag, directly or inherited from its catalog or schema;
 * catalogs and schemas are never members themselves. For the same tag, the assignment nearest to
 * the object wins: a table that carries {@code dept=hr} under a schema that carries {@code
 * dept=finance} is a member of the domain backed by {@code dept=hr} only.
 *
 * <p>A domain is either top-level or a sub-domain of exactly one top-level domain. Privileges on a
 * domain, and its ownership, also apply to its sub-domains: the metalake, the current parent and
 * the domain itself are checked together. An owner of any of them passes; otherwise a privilege
 * holds when it is allowed on at least one of them and denied on none, so a deny on a sub-domain is
 * not overridden by an allow on its parent.
 *
 * <p>A new domain is in {@link Status#DRAFT} status and, until it is published, is returned only to
 * the users who can manage it.
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
    /**
     * The domain is returned only to the users who can manage it, including through its parent or
     * the metalake.
     */
    DRAFT,
    /** The domain is returned to all users who can use it. */
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
   * <p>The domain refers to the tag itself, not to its name: a renamed tag keeps backing the
   * domain, and a new tag with the name of a deleted one does not.
   *
   * @return The backing tag of the domain, or empty if the backing tag has been removed or deleted
   *     and no new one has been set.
   */
  Optional<TagValue> backingTag();

  /**
   * Returns how the metadata object relates to this domain. It is set only on the domains returned
   * by {@link SupportsDomains#listDomainsInfo()}, and lists every relation that applies, with no
   * precedence among them.
   *
   * @return The relations of the metadata object to this domain, or an empty array if the domain is
   *     not listed for a metadata object.
   */
  DomainRelation[] relations();

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
   * Unpublishes the domain, making it visible only to the users who can manage it. A domain can be
   * unpublished only when it has no published sub-domains. Unpublishing a draft domain has no
   * effect.
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
   * <p>Since privileges on a parent, and its ownership, apply to its sub-domains, attaching or
   * moving changes who can see and manage this domain without any grant changing: those of the old
   * parent no longer apply to this domain, and those of the new parent do. A deny on this domain
   * still applies.
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
   * <p>Since privileges on a parent, and its ownership, apply to its sub-domains, detaching changes
   * who can see and manage this domain without any grant changing: those of the former parent no
   * longer apply to this domain.
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
   * @throws BackingTagAlreadyBoundException If the binding conflicts with the backing tag of
   *     another domain.
   * @throws ForbiddenException If the caller does not own the tag or the metalake.
   * @throws IllegalArgumentException If the value is not allowed by the tag.
   */
  Domain updateBackingTag(@Nullable TagValue backingTag)
      throws NoSuchDomainException, NoSuchTagException, BackingTagAlreadyBoundException,
          ForbiddenException, IllegalArgumentException;

  /**
   * Creates a sub-domain under this domain. The new sub-domain is in {@link Status#DRAFT} status.
   *
   * <p>The backing tag is always given explicitly. With {@code createBackingTag} set, a new tag
   * without values is created with the given name and owned by the caller; otherwise the given
   * existing tag, with its optional value, is bound. An existing tag is never adopted implicitly.
   *
   * @param name The name of the sub-domain.
   * @param comment The comment of the sub-domain, or null.
   * @param properties The properties of the sub-domain, or null.
   * @param backingTag The backing tag of the sub-domain: the name of the tag to create, or an
   *     existing tag with an optional value.
   * @param createBackingTag Whether to create the backing tag as a new tag.
   * @return The created sub-domain.
   * @throws NoSuchDomainException If this domain does not exist or is not visible to the caller.
   * @throws DomainAlreadyExistsException If a domain with the same name already exists.
   * @throws NoSuchTagException If the existing backing tag to bind does not exist.
   * @throws TagAlreadyExistsException If the backing tag to create already exists.
   * @throws BackingTagAlreadyBoundException If the binding conflicts with the backing tag of
   *     another domain.
   * @throws ForbiddenException If the caller does not own the existing backing tag or the metalake,
   *     or may not create tags.
   * @throws IllegalArgumentException If the backing tag is null, a value is given for a tag to
   *     create, the value is not allowed by the existing tag, this domain is itself a sub-domain,
   *     or the sub-domain cannot be created under this domain.
   */
  Domain createSubdomain(
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      TagValue backingTag,
      boolean createBackingTag)
      throws NoSuchDomainException, DomainAlreadyExistsException, NoSuchTagException,
          TagAlreadyExistsException, BackingTagAlreadyBoundException, ForbiddenException,
          IllegalArgumentException;

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
   * Lists the members in the scope of this domain that match the filter. The scope of a top-level
   * domain is its own members plus the members of its sub-domains visible to the caller; the scope
   * of a sub-domain is its own members. Only objects of the {@link DomainMember#SUPPORTED_TYPES
   * supported types} are members, and only the members the caller can access are returned.
   *
   * <p>A catalog or schema that carries a backing tag is expanded to the leaf objects under it that
   * Gravitino has recorded, and the catalog or schema itself is not listed. Objects of an external
   * catalog that Gravitino has not recorded yet are not listed. An object is listed once, with all
   * its {@link DomainMember#relations() relations} to this domain. The members of this domain
   * itself are the listed members whose relations include {@link DomainRelation.Type#DIRECT} or
   * {@link DomainRelation.Type#INHERITED}.
   *
   * @param filter The filter of the members. {@code DomainMemberFilter.builder().build()} lists the
   *     whole scope of this domain.
   * @return The members of this domain.
   * @throws NoSuchDomainException If this domain does not exist or is not visible to the caller.
   */
  DomainMember[] listMembers(DomainMemberFilter filter) throws NoSuchDomainException;
}
