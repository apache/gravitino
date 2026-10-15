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
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.Evolving;

/** A metadata object that is a member of a domain. */
@Evolving
public interface DomainMember {

  /** The metadata object types that can be members of a domain. */
  Set<MetadataObject.Type> SUPPORTED_TYPES =
      Sets.immutableEnumSet(
          MetadataObject.Type.CATALOG,
          MetadataObject.Type.SCHEMA,
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
   * Returns the sub-domain the member is reached through. A member of the listed domain itself is
   * reported as such even if it is also reached through a sub-domain. A member reached through
   * several sub-domains, and not a member of the listed domain itself, reports one of them.
   *
   * @return The name of the sub-domain, or empty if the object is a member of the listed domain
   *     itself.
   */
  Optional<String> subdomain();
}
