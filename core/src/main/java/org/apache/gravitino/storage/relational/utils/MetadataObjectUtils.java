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
package org.apache.gravitino.storage.relational.utils;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.po.FilesetPO;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.po.TopicPO;
import org.apache.gravitino.storage.relational.service.CatalogMetaService;
import org.apache.gravitino.storage.relational.service.FilesetMetaService;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.SchemaMetaService;
import org.apache.gravitino.storage.relational.service.TableMetaService;
import org.apache.gravitino.storage.relational.service.TopicMetaService;

/**
 * MetadataObjectUtils is used for converting full name to entity id and converting entity id to
 * full name.
 */
public class MetadataObjectUtils {

  private static final String DOT = ".";
  private static final Joiner DOT_JOINER = Joiner.on(DOT);
  private static final Splitter DOT_SPLITTER = Splitter.on(DOT);

  private MetadataObjectUtils() {}

  public static long getMetadataObjectId(
      String metalakeName, String fullName, MetadataObject.Type type) {
    if (fullName.equals(MetadataObjects.METADATA_OBJECT_RESERVED_NAME)
        && type == MetadataObject.Type.METALAKE) {
      return Entity.ALL_METALAKES_ENTITY_ID;
    }

    if (type == MetadataObject.Type.METALAKE) {
      return MetalakeMetaService.getInstance().getMetalakeIdByName(fullName);
    }

    String[] levelsWithoutMetalake = DOT_SPLITTER.splitToList(fullName).toArray(new String[0]);
    String[] fullLevels = ArrayUtils.addFirst(levelsWithoutMetalake, metalakeName);
    NameIdentifier identifier = NameIdentifier.of(fullLevels);

    if (type == MetadataObject.Type.CATALOG) {
      return CatalogMetaService.getInstance().getCatalogIdByNameIdentifier(identifier);
    } else if (type == MetadataObject.Type.SCHEMA) {
      return SchemaMetaService.getInstance().getSchemaIdByNameIdentifier(identifier);
    } else if (type == MetadataObject.Type.FILESET) {
      return FilesetMetaService.getInstance().getFilesetIdByNameIdentifier(identifier);
    } else if (type == MetadataObject.Type.TOPIC) {
      return TopicMetaService.getInstance().getTopicIdByNameIdentifier(identifier);
    } else if (type == MetadataObject.Type.TABLE) {
      return TableMetaService.getInstance().getTableByNameIdentifier(identifier);
    }

    throw new IllegalArgumentException(String.format("Doesn't support the type %s", type));
  }

  // Metadata object may be null because the metadata object can be deleted asynchronously.
  @Nullable
  public static String getMetadataObjectFullName(String type, long metadataObjectId) {
    if (type.equals(Entity.ALL_METALAKES_ENTITY_TYPE)) {
      return MetadataObjects.METADATA_OBJECT_RESERVED_NAME;
    }

    MetadataObject.Type metadatatype = MetadataObject.Type.valueOf(type);
    if (metadatatype == MetadataObject.Type.METALAKE) {
      MetalakePO metalakePO = MetalakeMetaService.getInstance().getMetalakePOById(metadataObjectId);
      if (metalakePO == null) {
        return null;
      }

      return metalakePO.getMetalakeName();
    }

    if (metadatatype == MetadataObject.Type.CATALOG) {
      return getCatalogFullName(metadataObjectId);
    }

    if (metadatatype == MetadataObject.Type.SCHEMA) {
      return getSchemaFullName(metadataObjectId);
    }

    if (metadatatype == MetadataObject.Type.TABLE) {
      TablePO tablePO = TableMetaService.getInstance().getTablePOById(metadataObjectId);
      if (tablePO == null) {
        return null;
      }

      String schemaName = getSchemaFullName(tablePO.getSchemaId());
      if (schemaName == null) {
        return null;
      }

      return DOT_JOINER.join(schemaName, tablePO.getTableName());
    }

    if (metadatatype == MetadataObject.Type.TOPIC) {
      TopicPO topicPO = TopicMetaService.getInstance().getTopicPOById(metadataObjectId);
      if (topicPO == null) {
        return null;
      }

      String schemaName = getSchemaFullName(topicPO.getSchemaId());
      if (schemaName == null) {
        return null;
      }

      return DOT_JOINER.join(schemaName, topicPO.getTopicName());
    }

    if (metadatatype == MetadataObject.Type.FILESET) {
      FilesetPO filesetPO = FilesetMetaService.getInstance().getFilesetPOById(metadataObjectId);
      if (filesetPO == null) {
        return null;
      }

      String schemaName = getSchemaFullName(filesetPO.getSchemaId());
      if (schemaName == null) {
        return null;
      }

      return DOT_JOINER.join(schemaName, filesetPO.getFilesetName());
    }

    throw new IllegalArgumentException(String.format("Doesn't support the type %s", metadatatype));
  }

  @Nullable
  private static String getCatalogFullName(Long entityId) {
    CatalogPO catalogPO = CatalogMetaService.getInstance().getCatalogPOById(entityId);
    if (catalogPO == null) {
      return null;
    }
    return catalogPO.getCatalogName();
  }

  @Nullable
  private static String getSchemaFullName(Long entityId) {
    SchemaPO schemaPO = SchemaMetaService.getInstance().getSchemaPOById(entityId);

    if (schemaPO == null) {
      return null;
    }

    String catalogName = getCatalogFullName(schemaPO.getCatalogId());
    if (catalogName == null) {
      return null;
    }

    return DOT_JOINER.join(catalogName, schemaPO.getSchemaName());
  }
}
