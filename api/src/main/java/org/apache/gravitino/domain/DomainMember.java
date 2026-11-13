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

import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.Evolving;

/**
 * A metadata object that is a member of a domain.
 *
 * <p>By default a domain lists its direct members: the objects that carry its backing tag
 * themselves. A direct member is a leaf object of the {@link #SUPPORTED_TYPES supported types} or a
 * catalog or schema of the {@link #CONTAINER_TYPES container types}. A catalog or schema member is
 * listed once and includes its children: the leaf objects under it inherit the backing tag unless a
 * nearer assignment of the same tag overrides it.
 *
 * <p>When the listing is {@link DomainMemberFilter#expand() expanded}, a catalog or schema member
 * is replaced by the recorded leaf objects under it that inherit the backing tag, so only leaf
 * objects are listed.
 */
@Evolving
public interface DomainMember {

  /**
   * The leaf metadata object types that can be members of a domain. These are the only types listed
   * by an {@link DomainMemberFilter#expand() expanded} listing, and the only types whose domains
   * can be looked up with {@link SupportsDomains}.
   */
  Set<MetadataObject.Type> SUPPORTED_TYPES =
      Sets.immutableEnumSet(
          MetadataObject.Type.TABLE,
          MetadataObject.Type.VIEW,
          MetadataObject.Type.FILESET,
          MetadataObject.Type.TOPIC,
          MetadataObject.Type.MODEL,
          MetadataObject.Type.SEMANTIC_MODEL);

  /**
   * The container metadata object types that can be direct members of a domain. A member of these
   * types includes its children, and is listed only when the listing is not {@link
   * DomainMemberFilter#expand() expanded}.
   */
  Set<MetadataObject.Type> CONTAINER_TYPES =
      Sets.immutableEnumSet(MetadataObject.Type.CATALOG, MetadataObject.Type.SCHEMA);

  /**
   * Returns the metadata object that is a member of the domain.
   *
   * @return The metadata object.
   */
  MetadataObject object();

  /**
   * Returns how the member relates to the listed domain. The {@link DomainMemberFilter} selects the
   * members only: every relation of a listed member to the listed domain is reported, with no
   * precedence among them, whatever domains the filter selects. For example, in an expanded listing
   * when schema {@code cat.db} carries the backing tag of "Finance" and table {@code cat.db.trades}
   * carries that of its sub-domain "Trading", listing Finance reports the table as {@link
   * DomainRelation.Type#INHERITED} from {@code cat.db} and {@link
   * DomainRelation.Type#VIA_SUBDOMAIN} Trading. The member belongs to the listed domain itself when
   * its relations include {@link DomainRelation.Type#DIRECT} or {@link
   * DomainRelation.Type#INHERITED}.
   *
   * <p>Without expansion, a member carries the backing tag itself, so its relations are only {@link
   * DomainRelation.Type#DIRECT} and {@link DomainRelation.Type#VIA_SUBDOMAIN}. The example above
   * then lists schema {@code cat.db} as DIRECT and table {@code cat.db.trades} as VIA_SUBDOMAIN
   * Trading.
   *
   * @return The relations of the member to the listed domain, never empty.
   */
  DomainRelation[] relations();
}
