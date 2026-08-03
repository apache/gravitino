/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.cache;

import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;

/** Capability for invalidating a locally cached metadata entity after an out-of-band mutation. */
public interface SupportsEntityCacheInvalidation {

  /**
   * Invalidates one local entity-cache entry.
   *
   * @param identifier entity name identifier
   * @param type entity type
   * @return {@code true} when invalidation was accepted
   */
  boolean invalidateEntityCache(NameIdentifier identifier, Entity.EntityType type);
}
