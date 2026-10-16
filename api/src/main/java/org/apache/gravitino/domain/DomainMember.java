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
 * A metadata object that is a member of a domain. Only leaf objects, of the {@link #SUPPORTED_TYPES
 * supported types}, are members. Catalogs and schemas are never members: a backing tag on a catalog
 * or schema makes the leaf objects under it members through tag inheritance.
 */
@Evolving
public interface DomainMember {

  /** The metadata object types that can be members of a domain, all of them leaf objects. */
  Set<MetadataObject.Type> SUPPORTED_TYPES =
      Sets.immutableEnumSet(
          MetadataObject.Type.TABLE,
          MetadataObject.Type.VIEW,
          MetadataObject.Type.FILESET,
          MetadataObject.Type.TOPIC,
          MetadataObject.Type.MODEL,
          MetadataObject.Type.SEMANTIC_MODEL);

  /**
   * Returns the metadata object that is a member of the domain.
   *
   * @return The metadata object.
   */
  MetadataObject object();

  /**
   * Returns how the member relates to the listed domain. The {@link DomainMemberFilter} selects the
   * members only: every relation of a listed member to the listed domain is reported, with no
   * precedence among them, whatever domains the filter selects. For example, when schema {@code
   * cat.db} carries the backing tag of "Finance" and table {@code cat.db.trades} carries that of
   * its sub-domain "Trading", listing Finance reports the table as {@link
   * DomainRelation.Type#INHERITED} from {@code cat.db} and {@link
   * DomainRelation.Type#VIA_SUBDOMAIN} Trading. The member belongs to the listed domain itself when
   * its relations include {@link DomainRelation.Type#DIRECT} or {@link
   * DomainRelation.Type#INHERITED}.
   *
   * @return The relations of the member to the listed domain, never empty.
   */
  DomainRelation[] relations();
}
