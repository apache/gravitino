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
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.tag.TagValue;

/**
 * A domain groups metadata objects from any catalog under a business name, such as "Finance". A
 * metadata object is a member of a domain when it carries the domain's backing tag, directly or
 * inherited from its catalog or schema.
 *
 * <p>A domain is either top-level or a sub-domain of exactly one top-level domain. A new domain is
 * in {@link Status#DRAFT} status and is visible only to its owner and managers until it is
 * published.
 *
 * <p>A {@code Domain} is a snapshot. The operations on it that change the domain itself, such as
 * {@link #publish()}, return a new snapshot and leave this one unchanged.
 */
@Evolving
public interface Domain extends Auditable {

  /** The status of a domain. */
  enum Status {
    /** The domain is visible only to its owner and managers. */
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
   * Returns the name of the parent domain.
   *
   * @return The name of the parent domain, or empty if the domain is top-level.
   */
  Optional<String> parent();

  /**
   * Returns the backing tag of the domain. The metadata objects that carry this tag, with this
   * value if one is set, are the members of the domain.
   *
   * @return The backing tag of the domain, or empty if the backing tag has been deleted and no new
   *     one has been set.
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
   * @throws NoSuchDomainException If the domain does not exist.
   * @throws IllegalArgumentException If the domain is a sub-domain and its parent is not published.
   */
  Domain publish() throws NoSuchDomainException, IllegalArgumentException;

  /**
   * Unpublishes the domain, making it visible only to its owner and managers. A domain can be
   * unpublished only when it has no published sub-domains. Unpublishing a draft domain has no
   * effect.
   *
   * @return The unpublished domain.
   * @throws NoSuchDomainException If the domain does not exist.
   * @throws IllegalArgumentException If the domain has published sub-domains.
   */
  Domain unpublish() throws NoSuchDomainException, IllegalArgumentException;

  /**
   * Creates a sub-domain under this domain. The new sub-domain is in {@link Status#DRAFT} status.
   * If no backing tag is given, a tag named after the domain path, such as "Finance/Trading", is
   * created and used as the backing tag.
   *
   * @param name The name of the sub-domain.
   * @param comment The comment of the sub-domain, or null.
   * @param properties The properties of the sub-domain, or null.
   * @param backingTag The existing tag, with an optional value, that backs the sub-domain, or null
   *     to create the default backing tag.
   * @return The created sub-domain.
   * @throws NoSuchDomainException If this domain does not exist.
   * @throws DomainAlreadyExistsException If a domain with the same name already exists.
   * @throws IllegalArgumentException If this domain is itself a sub-domain, or the sub-domain
   *     cannot be created under this domain.
   */
  Domain createSubdomain(
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      @Nullable TagValue backingTag)
      throws NoSuchDomainException, DomainAlreadyExistsException, IllegalArgumentException;

  /**
   * Attaches an existing domain as a sub-domain of this domain. If the domain is a sub-domain of
   * another domain, it is moved under this domain. Attaching a sub-domain of this domain has no
   * effect.
   *
   * @param name The name of the domain to attach.
   * @throws NoSuchDomainException If this domain or the domain to attach does not exist.
   * @throws IllegalArgumentException If the domain cannot be attached, for example because this
   *     domain is a sub-domain, the domain to attach has sub-domains, or the domain to attach is
   *     published and this domain is not.
   */
  void attachSubdomain(String name) throws NoSuchDomainException, IllegalArgumentException;

  /**
   * Detaches a sub-domain from this domain. The detached domain becomes top-level.
   *
   * @param name The name of the sub-domain to detach.
   * @return True if the sub-domain is detached, false if it is not a sub-domain of this domain.
   * @throws NoSuchDomainException If this domain does not exist.
   */
  boolean detachSubdomain(String name) throws NoSuchDomainException;

  /**
   * Lists the names of the sub-domains of this domain.
   *
   * @return The names of the sub-domains.
   * @throws NoSuchDomainException If this domain does not exist.
   */
  String[] listSubdomains() throws NoSuchDomainException;

  /**
   * Lists the sub-domains of this domain with details.
   *
   * @return The sub-domains.
   * @throws NoSuchDomainException If this domain does not exist.
   */
  Domain[] listSubdomainsInfo() throws NoSuchDomainException;

  /**
   * Lists the members of this domain that match the filter. Only the members the caller can access
   * are returned.
   *
   * @param filter The filter of the members, use {@code DomainMemberFilter.builder().build()} to
   *     list all members, including those of the sub-domains.
   * @return The members of this domain.
   * @throws NoSuchDomainException If this domain does not exist.
   */
  DomainMember[] listMembers(DomainMemberFilter filter) throws NoSuchDomainException;
}
