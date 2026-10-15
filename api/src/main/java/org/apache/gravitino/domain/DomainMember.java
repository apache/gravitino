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

import java.util.Optional;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.Evolving;

/** A metadata object that is a member of a domain. */
@Evolving
public interface DomainMember {

  /**
   * Returns the metadata object that is a member of the domain.
   *
   * @return The metadata object.
   */
  MetadataObject object();

  /**
   * Returns the sub-domain the member is reached through.
   *
   * @return The name of the sub-domain, or empty if the object is a member of the listed domain
   *     itself.
   */
  Optional<String> subdomain();
}
