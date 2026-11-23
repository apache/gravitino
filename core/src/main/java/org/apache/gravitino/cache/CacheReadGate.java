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
package org.apache.gravitino.cache;

import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;

/**
 * Decides whether a cache hit may be used when resolving or reading an entity. Store-side pending
 * writes and mid-publication entries can temporarily disallow cache reads for a key.
 */
@FunctionalInterface
public interface CacheReadGate {

  /** Always allows cache hits. */
  CacheReadGate ALWAYS_ALLOW = (ident, type) -> true;

  /**
   * Returns whether a cached value for {@code ident} may be used.
   *
   * @param ident The entity name identifier.
   * @param type The entity type.
   * @return {@code true} if a cache hit is allowed.
   */
  boolean allowCacheRead(NameIdentifier ident, Entity.EntityType type);
}
