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
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TopicEntity;

public class CacheKey {
  private NameIdentifier nameIdent;
  private long id;
  private Entity.EntityType entityType;

  public CacheKey(Entity entity) {
    this.entityType = entity.type();
    switch (entity.type()) {
      case METALAKE:
        BaseMetalake metalakeEntity = (BaseMetalake) entity;
        this.id = metalakeEntity.id();
        this.nameIdent = NameIdentifier.of(metalakeEntity.namespace(), metalakeEntity.name());
        break;

      case CATALOG:
        CatalogEntity catalogEntity = (CatalogEntity) entity;
        this.id = catalogEntity.id();
        this.nameIdent = NameIdentifier.of(catalogEntity.namespace(), catalogEntity.name());
        break;

      case SCHEMA:
        SchemaEntity schemaEntity = (SchemaEntity) entity;
        this.id = schemaEntity.id();
        this.nameIdent = NameIdentifier.of(schemaEntity.namespace(), schemaEntity.name());
        break;

      case TABLE:
        TableEntity tableEntity = (TableEntity) entity;
        this.id = tableEntity.id();
        this.nameIdent = NameIdentifier.of(tableEntity.namespace(), tableEntity.name());
        break;

      case FILESET:
        FilesetEntity filesetEntity = (FilesetEntity) entity;
        this.id = filesetEntity.id();
        this.nameIdent = NameIdentifier.of(filesetEntity.namespace(), filesetEntity.name());
        break;

      case MODEL:
        ModelEntity modelEntity = (ModelEntity) entity;
        this.id = modelEntity.id();
        this.nameIdent = NameIdentifier.of(modelEntity.namespace(), modelEntity.name());
        break;

      case TOPIC:
        TopicEntity topicEntity = (TopicEntity) entity;
        this.id = topicEntity.id();
        this.nameIdent = NameIdentifier.of(topicEntity.namespace(), topicEntity.name());
        break;

      case MODEL_VERSION:
        ModelVersionEntity modelVersionEntity = (ModelVersionEntity) entity;
        this.id = modelVersionEntity.id();
        this.nameIdent =
            NameIdentifier.of(modelVersionEntity.namespace(), modelVersionEntity.name());
        break;

      default:
        break;
    }
  }

  public long id() {
    return id;
  }

  public NameIdentifier nameIdent() {
    return nameIdent;
  }

  public Entity.EntityType entityType() {
    return entityType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CacheKey)) {
      return false;
    }
    CacheKey cacheKey = (CacheKey) o;
    return id == cacheKey.id && nameIdent.equals(cacheKey.nameIdent);
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(id);
    result = 31 * result + nameIdent.hashCode();

    return result;
  }

  @Override
  public String toString() {
    return "CacheKey{"
        + "id="
        + id
        + ", nameIdent="
        + nameIdent
        + ", entityType="
        + entityType
        + '}';
  }
}
