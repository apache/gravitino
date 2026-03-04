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
package org.apache.gravitino.meta;

import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;

/** Interface for resolving entity IDs based on NameIdentifiers and Entity types. */
public interface EntityIdResolver {

  /**
   * Get EntityIds for the given NameIdentifier and Entity type.
   *
   * @param nameIdentifier NameIdentifier of the entity.
   * @param type Entity type.
   * @return EntityIds corresponding to the NameIdentifier and Entity type.
   */
  NamespacedEntityId getEntityIds(NameIdentifier nameIdentifier, Entity.EntityType type);

  /**
   * Get the entity ID for the given NameIdentifier and Entity type.
   *
   * @param nameIdentifier NameIdentifier of the entity.
   * @param type Entity type.
   * @return Entity ID corresponding to the NameIdentifier and Entity type.
   */
  Long getEntityId(NameIdentifier nameIdentifier, Entity.EntityType type);
}
