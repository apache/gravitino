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

package org.apache.gravitino.storage.relational.service;

import com.google.common.base.Preconditions;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.EntityIdResolver;
import org.apache.gravitino.meta.NamespacedEntityId;

/**
 * Service class to resolve entity IDs based on NameIdentifiers and Entity types. This class must be
 * initialized with an EntityIdResolver before use.
 */
public class EntityIdService {
  private static EntityIdResolver entityIdResolver;

  private EntityIdService() {}

  public static void initialize(EntityIdResolver idResolver) {
    entityIdResolver = idResolver;
  }

  public static long getEntityId(NameIdentifier identifier, Entity.EntityType type) {
    Preconditions.checkArgument(entityIdResolver != null, "EntityIdService is not initialized");
    return entityIdResolver.getEntityId(identifier, type);
  }

  public static NamespacedEntityId getEntityIds(NameIdentifier identifier, Entity.EntityType type) {
    Preconditions.checkArgument(entityIdResolver != null, "EntityIdService is not initialized");
    return entityIdResolver.getEntityIds(identifier, type);
  }
}
