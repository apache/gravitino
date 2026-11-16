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

import org.apache.gravitino.annotation.Evolving;

/**
 * Interface for listing the domains of a leaf metadata object, such as a table, view or fileset.
 * Catalogs and schemas do not support it, since they are never members of a domain; the domain a
 * catalog or schema passes down to the objects under it is visible from its tags.
 *
 * <p>The domains listed are all the domains visible to the caller whose scope contains the object:
 * the domains whose backing tag the object's effective tags match, and the parents of those that
 * are sub-domains visible to the caller. The effective tags are the object's own tags plus those
 * inherited from its schema and catalog; for the same tag, the assignment nearest to the object
 * wins. Each domain comes with its {@link Domain#relations() relations} to the object; an object
 * tagged only with the backing tag of sub-domain "Trading" lists Trading as {@link
 * DomainRelation.Type#DIRECT} and its parent "Finance" as {@link DomainRelation.Type#VIA_SUBDOMAIN}
 * Trading.
 *
 * <p>To add an object to a domain or remove it, associate the domain's backing tag with the object
 * or remove it, through {@link org.apache.gravitino.tag.SupportsTags}. That changes only the tags
 * assigned to the object itself, and the domains are then derived again from its effective tags. An
 * object that inherits the backing tag from its schema or catalog stays in the domain until the tag
 * is removed there, and removing a tag from the object can make an inherited assignment of the same
 * tag take effect again.
 */
@Evolving
public interface SupportsDomains {

  /**
   * Lists the names of the domains whose scope contains the object.
   *
   * @return The names of the domains.
   */
  String[] listDomains();

  /**
   * Lists the domains whose scope contains the object, with details. {@link Domain#relations()} of
   * each domain returns how the object relates to it.
   *
   * @return The domains.
   */
  Domain[] listDomainsInfo();
}
