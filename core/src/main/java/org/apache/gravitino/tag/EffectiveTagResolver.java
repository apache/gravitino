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
package org.apache.gravitino.tag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves effective tags for a metadata object using direct-assignment override semantics.
 *
 * <p>The requested object is evaluated first, followed by its ancestors from nearest to outermost.
 * The first assignment for a tag name wins, so a direct or nearer assignment overrides a more
 * distant inherited assignment, including its assignment values.
 */
public class EffectiveTagResolver {

  private static final Logger LOG = LoggerFactory.getLogger(EffectiveTagResolver.class);

  private final EntityStore entityStore;

  /**
   * Creates an effective tag resolver.
   *
   * @param entityStore The entity store used to read tag relations.
   */
  public EffectiveTagResolver(EntityStore entityStore) {
    this.entityStore = entityStore;
  }

  /**
   * Resolves effective tags for a metadata object.
   *
   * @param metalake The metalake name.
   * @param metadataObject The metadata object.
   * @return Effective tags in deterministic nearest-assignment order.
   */
  public TagEntity[] resolve(String metalake, MetadataObject metadataObject) {
    MetadataObjectUtil.checkMetadataObject(metalake, metadataObject);
    List<MetadataObject> resolutionOrder = new ArrayList<>();
    resolutionOrder.add(metadataObject);
    resolutionOrder.addAll(MetadataObjectUtil.getParentMetadataObjects(metadataObject));

    Map<String, TagEntity> effectiveTags = new LinkedHashMap<>();
    for (MetadataObject object : resolutionOrder) {
      NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, object);
      Entity.EntityType entityType = MetadataObjectUtil.toEntityType(object);
      try {
        List<TagEntity> tags =
            entityStore
                .relationOperations()
                .listEntitiesByRelation(
                    SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
                    identifier,
                    entityType);
        tags.forEach(tag -> effectiveTags.putIfAbsent(tag.name(), tag));
      } catch (NoSuchEntityException e) {
        throw new NoSuchMetadataObjectException(
            e, "Failed to resolve effective tags for metadata object %s due to not found", object);
      } catch (IOException e) {
        LOG.error("Failed to resolve effective tags for metadata object {}", object, e);
        throw new RuntimeException(e);
      }
    }
    return effectiveTags.values().toArray(new TagEntity[0]);
  }
}
