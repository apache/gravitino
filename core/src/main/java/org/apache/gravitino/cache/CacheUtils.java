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

import java.util.HashSet;
import java.util.Set;
import org.apache.commons.collections4.Trie;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TopicEntity;

public class CacheUtils {
  public static <E extends Entity & HasIdentifier> Class<E> getEntityClass(Entity.EntityType type) {
    if (type == null) {
      throw new IllegalArgumentException("EntityType must not be null");
    }
    switch (type) {
      case METALAKE:
        return (Class<E>) BaseMetalake.class;

      case CATALOG:
        return (Class<E>) CatalogEntity.class;

      case SCHEMA:
        return (Class<E>) SchemaEntity.class;

      case TABLE:
        return (Class<E>) TableEntity.class;

      case FILESET:
        return (Class<E>) FilesetEntity.class;

      case MODEL:
        return (Class<E>) ModelEntity.class;

      case TOPIC:
        return (Class<E>) TopicEntity.class;

      case MODEL_VERSION:
        return (Class<E>) ModelVersionEntity.class;

      default:
        throw new IllegalArgumentException("Unsupported EntityType: " + type.getShortName());
    }
  }

  public static void deleteByPrefix(Trie<String, ?> trie, String prefix) {
    Set<String> keysToRemove = new HashSet<>(trie.prefixMap(prefix).keySet());

    for (String key : keysToRemove) {
      trie.remove(key);
    }
  }
}
