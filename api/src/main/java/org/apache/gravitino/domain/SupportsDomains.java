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
 * Interface for listing the domains of a metadata object, such as a catalog, schema or table.
 *
 * <p>An object belongs to a domain when its effective tags include the domain's backing tag. The
 * effective tags are its own tags plus those inherited from its schema and catalog; for the same
 * tag, the assignment nearest to the object wins. Only the domains visible to the caller are
 * listed, and a parent domain is not listed for the members of its sub-domains.
 *
 * <p>To add an object to a domain or remove it, associate the domain's backing tag with the object
 * or remove it, through {@link org.apache.gravitino.tag.SupportsTags}.
 */
@Evolving
public interface SupportsDomains {

  /**
   * Lists the names of the domains the object belongs to, directly or through inheritance.
   *
   * @return The names of the domains.
   */
  String[] listDomains();

  /**
   * Lists the domains the object belongs to with details, directly or through inheritance. For an
   * inherited membership, {@link Domain#inheritedFrom()} returns the catalog or schema it is
   * inherited from.
   *
   * @return The domains.
   */
  Domain[] listDomainsInfo();
}
