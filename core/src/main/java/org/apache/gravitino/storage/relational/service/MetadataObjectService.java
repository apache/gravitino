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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.po.ColumnPO;
import org.apache.gravitino.storage.relational.po.FilesetPO;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.po.TopicPO;

/**
 * MetadataObjectService is used for converting full name to entity id and converting entity id to
 * full name.
 */
public class MetadataObjectService {

  private static final String DOT = ".";
  private static final Joiner DOT_JOINER = Joiner.on(DOT);
  private static final Splitter DOT_SPLITTER = Splitter.on(DOT);

  private MetadataObjectService() {}

  public static long getMetadataObjectId(
      long metalakeId, String fullName, MetadataObject.Type type) {
    if (type == MetadataObject.Type.METALAKE) {
      return MetalakeMetaService.getInstance().getMetalakeIdByName(fullName);
    }

    List<String> names = DOT_SPLITTER.splitToList(fullName);
    if (type == MetadataObject.Type.ROLE) {
      return RoleMetaService.getInstance().getRoleIdByMetalakeIdAndName(metalakeId, names.get(0));
    }

    long catalogId =
        CatalogMetaService.getInstance().getCatalogIdByMetalakeIdAndName(metalakeId, names.get(0));
    if (type == MetadataObject.Type.CATALOG) {
      return catalogId;
    }

    long schemaId =
        SchemaMetaService.getInstance().getSchemaIdByCatalogIdAndName(catalogId, names.get(1));
    if (type == MetadataObject.Type.SCHEMA) {
      return schemaId;
    }

    if (type == MetadataObject.Type.FILESET) {
      return FilesetMetaService.getInstance().getFilesetIdBySchemaIdAndName(schemaId, names.get(2));
    } else if (type == MetadataObject.Type.TOPIC) {
      return TopicMetaService.getInstance().getTopicIdBySchemaIdAndName(schemaId, names.get(2));
    }

    long tableId =
        TableMetaService.getInstance().getTableIdBySchemaIdAndName(schemaId, names.get(2));
    if (type == MetadataObject.Type.TABLE) {
      return tableId;
    }

    if (type == MetadataObject.Type.COLUMN) {
      return TableColumnMetaService.getInstance()
          .getColumnIdByTableIdAndName(tableId, names.get(3));
    }

    throw new IllegalArgumentException(String.format("Doesn't support the type %s", type));
  }

  // Metadata object may be null because the metadata object can be deleted asynchronously.
  @Nullable
  public static String getMetadataObjectFullName(String type, long metadataObjectId) {
    MetadataObject.Type metadataType = MetadataObject.Type.valueOf(type);
    String fullName = null;
    long objectId = metadataObjectId;

    do {
      switch (metadataType) {
        case METALAKE:
          MetalakePO metalakePO = MetalakeMetaService.getInstance().getMetalakePOById(objectId);
          if (metalakePO != null) {
            fullName = metalakePO.getMetalakeName();
            metadataType = null;
          } else {
            return null;
          }
          break;

        case CATALOG:
          CatalogPO catalogPO = CatalogMetaService.getInstance().getCatalogPOById(objectId);
          if (catalogPO != null) {
            fullName =
                fullName != null
                    ? DOT_JOINER.join(catalogPO.getCatalogName(), fullName)
                    : catalogPO.getCatalogName();
            metadataType = null;
          } else {
            return null;
          }
          break;

        case SCHEMA:
          SchemaPO schemaPO = SchemaMetaService.getInstance().getSchemaPOById(objectId);
          if (schemaPO != null) {
            fullName =
                fullName != null
                    ? DOT_JOINER.join(schemaPO.getSchemaName(), fullName)
                    : schemaPO.getSchemaName();
            objectId = schemaPO.getCatalogId();
            metadataType = MetadataObject.Type.CATALOG;
          } else {
            return null;
          }
          break;

        case TABLE:
          TablePO tablePO = TableMetaService.getInstance().getTablePOById(objectId);
          if (tablePO != null) {
            fullName =
                fullName != null
                    ? DOT_JOINER.join(tablePO.getTableName(), fullName)
                    : tablePO.getTableName();
            objectId = tablePO.getSchemaId();
            metadataType = MetadataObject.Type.SCHEMA;
          } else {
            return null;
          }
          break;

        case TOPIC:
          TopicPO topicPO = TopicMetaService.getInstance().getTopicPOById(objectId);
          if (topicPO != null) {
            fullName =
                fullName != null
                    ? DOT_JOINER.join(topicPO.getTopicName(), fullName)
                    : topicPO.getTopicName();
            objectId = topicPO.getSchemaId();
            metadataType = MetadataObject.Type.SCHEMA;
          } else {
            return null;
          }
          break;

        case FILESET:
          FilesetPO filesetPO = FilesetMetaService.getInstance().getFilesetPOById(objectId);
          if (filesetPO != null) {
            fullName =
                fullName != null
                    ? DOT_JOINER.join(filesetPO.getFilesetName(), fullName)
                    : filesetPO.getFilesetName();
            objectId = filesetPO.getSchemaId();
            metadataType = MetadataObject.Type.SCHEMA;
          } else {
            return null;
          }
          break;

        case COLUMN:
          ColumnPO columnPO = TableColumnMetaService.getInstance().getColumnPOById(objectId);
          if (columnPO != null) {
            fullName =
                fullName != null
                    ? DOT_JOINER.join(columnPO.getColumnName(), fullName)
                    : columnPO.getColumnName();
            objectId = columnPO.getTableId();
            metadataType = MetadataObject.Type.TABLE;
          } else {
            return null;
          }
          break;

        default:
          throw new IllegalArgumentException(
              String.format("Doesn't support the type %s", metadataType));
      }
    } while (metadataType != null);

    return fullName;
  }
}
