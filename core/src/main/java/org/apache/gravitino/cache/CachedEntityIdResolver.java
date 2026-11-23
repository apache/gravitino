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
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.EntityIdResolver;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.utils.NameIdentifierUtil;

public class CachedEntityIdResolver implements EntityIdResolver {

  private final EntityCache entityCache;
  private final EntityIdResolver underlyingResolver;

  public CachedEntityIdResolver(EntityCache entityCache, EntityIdResolver underlyingResolver) {
    this.entityCache = entityCache;
    this.underlyingResolver = underlyingResolver;
  }

  @Override
  public NamespacedEntityId getEntityIds(NameIdentifier nameIdentifier, Entity.EntityType type) {
    return entityCache
        .getIfPresent(nameIdentifier, type)
        .map(
            entity -> {
              if (nameIdentifier.hasNamespace()) {
                NamespacedEntityId namespaceIds =
                    getEntityIds(
                        NameIdentifierUtil.parentNameIdentifier(nameIdentifier, type),
                        NameIdentifierUtil.parentEntityType(type));
                return new NamespacedEntityId(entity.id(), namespaceIds.fullIds());
              } else {
                return new NamespacedEntityId(entity.id());
              }
            })
        .orElseGet(() -> underlyingResolver.getEntityIds(nameIdentifier, type));
  }

  @Override
  public Long getEntityId(NameIdentifier nameIdentifier, Entity.EntityType type) {
    return entityCache
        .getIfPresent(nameIdentifier, type)
        .map(HasIdentifier::id)
        .orElseGet(() -> underlyingResolver.getEntityId(nameIdentifier, type));
  }
}
