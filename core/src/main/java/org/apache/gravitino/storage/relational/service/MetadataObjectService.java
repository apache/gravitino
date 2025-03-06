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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.po.ColumnPO;
import org.apache.gravitino.storage.relational.po.FilesetPO;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.ModelPO;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.po.TopicPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetadataObjectService is used for converting full name to entity id and converting entity id to
 * full name.
 */
public class MetadataObjectService {

  private static final String DOT = ".";
  private static final Joiner DOT_JOINER = Joiner.on(DOT);
  private static final Splitter DOT_SPLITTER = Splitter.on(DOT);

  private static final Logger LOG = LoggerFactory.getLogger(MetadataObjectService.class);

  private MetadataObjectService() {}

  public static long getMetadataObjectId(
      long metalakeId, String fullName, MetadataObject.Type type) {
    if (type == MetadataObject.Type.METALAKE) {
      return MetalakeMetaService.getInstance().getMetalakeIdByName(fullName);
    }

    if (type == MetadataObject.Type.ROLE) {
      return RoleMetaService.getInstance().getRoleIdByMetalakeIdAndName(metalakeId, fullName);
    }
    List<String> names = DOT_SPLITTER.splitToList(fullName);

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
    } else if (type == MetadataObject.Type.MODEL) {
      return ModelMetaService.getInstance()
          .getModelIdBySchemaIdAndModelName(schemaId, names.get(2));
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
            // if fullName is null:
            // fullName = catalogPO.getCatalogName(),schemaPO.getSchemaName()
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
          // if fullName is null:
          // fullName = catalogPO.getSchemaName(),schemaPO.getTableName()
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

        case MODEL:
          ModelPO modelPO = ModelMetaService.getInstance().getModelPOById(objectId);
          if (modelPO != null) {
            fullName =
                fullName != null
                    ? DOT_JOINER.join(modelPO.getModelName(), fullName)
                    : modelPO.getModelName();
            objectId = modelPO.getSchemaId();
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

  public static Map<Long, String> getMetalakeObjectFullNames(List<Long> ids) {
    List<MetalakePO> metalakePOs =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.listMetalakePOsByMetalakeIds(ids));

    if (metalakePOs == null || metalakePOs.isEmpty()) {
      return new HashMap<>();
    }

    HashMap<Long, String> metalakeIdAndNameMap = new HashMap<>();

    metalakePOs.forEach(
        metalakePO -> {
          if (metalakePO.getMetalakeId() == null) {
            metalakeIdAndNameMap.put(metalakePO.getMetalakeId(), null);
            return;
          }
          metalakeIdAndNameMap.put(metalakePO.getMetalakeId(), metalakePO.getMetalakeName());
        });

    return metalakeIdAndNameMap;
  }

  public static Map<Long, String> getFilesetObjectFullNames(List<Long> ids) {
    List<FilesetPO> filesetPOs =
        SessionUtils.getWithoutCommit(
            FilesetMetaMapper.class, mapper -> mapper.listFilesetPOsByFilesetIds(ids));

    if (filesetPOs == null || filesetPOs.isEmpty()) {
      return new HashMap<>();
    }

    List<Long> catalogIds =
        filesetPOs.stream().map(FilesetPO::getCatalogId).collect(Collectors.toList());
    List<Long> schemaIds =
        filesetPOs.stream().map(FilesetPO::getSchemaId).collect(Collectors.toList());

    Map<Long, String> catalogIdAndNameMap = getCatalogIdAndNameMap(catalogIds);
    Map<Long, String> schemaIdAndNameMap = getSchemaIdAndNameMap(schemaIds);

    HashMap<Long, String> filesetIdAndNameMap = new HashMap<>();

    filesetPOs.forEach(
        filesetPO -> {
          // since the catalog or schema can be deleted, we need to check the null value,
          // and when catalog or schema is deleted, we will set fullName of filesetPO to null
          String catalogName = catalogIdAndNameMap.getOrDefault(filesetPO.getCatalogId(), null);
          if (catalogName == null) {
            LOG.warn("The catalog of fileset {} may be deleted", filesetPO.getFilesetId());
            filesetIdAndNameMap.put(filesetPO.getFilesetId(), null);
            return;
          }

          String schemaName = schemaIdAndNameMap.getOrDefault(filesetPO.getSchemaId(), null);
          if (schemaName == null) {
            LOG.warn("The schema of fileset {} may be deleted", filesetPO.getFilesetId());
            filesetIdAndNameMap.put(filesetPO.getFilesetId(), null);
            return;
          }

          String fullName = DOT_JOINER.join(catalogName, schemaName, filesetPO.getFilesetName());
          filesetIdAndNameMap.put(filesetPO.getFilesetId(), fullName);
        });

    return filesetIdAndNameMap;
  }

  public static Map<Long, String> getModelObjectFullNames(List<Long> ids) {
    List<ModelPO> modelPOs =
        SessionUtils.getWithoutCommit(
            ModelMetaMapper.class, mapper -> mapper.listModelPOsByModelIds(ids));

    if (modelPOs == null || modelPOs.isEmpty()) {
      return new HashMap<>();
    }

    List<Long> catalogIds =
        modelPOs.stream().map(ModelPO::getCatalogId).collect(Collectors.toList());
    List<Long> schemaIds = modelPOs.stream().map(ModelPO::getSchemaId).collect(Collectors.toList());

    Map<Long, String> catalogIdAndNameMap = getCatalogIdAndNameMap(catalogIds);
    Map<Long, String> schemaIdAndNameMap = getSchemaIdAndNameMap(schemaIds);

    HashMap<Long, String> modelIdAndNameMap = new HashMap<>();

    modelPOs.forEach(
        modelPO -> {
          // since the catalog or schema can be deleted, we need to check the null value,
          // and when catalog or schema is deleted, we will set fullName of modelPO to null
          String catalogName = catalogIdAndNameMap.getOrDefault(modelPO.getCatalogId(), null);
          if (catalogName == null) {
            LOG.warn("The catalog of model {} may be deleted", modelPO.getModelId());
            modelIdAndNameMap.put(modelPO.getModelId(), null);
            return;
          }

          String schemaName = schemaIdAndNameMap.getOrDefault(modelPO.getSchemaId(), null);
          if (schemaName == null) {
            LOG.warn("The schema of model {} may be deleted", modelPO.getModelId());
            modelIdAndNameMap.put(modelPO.getModelId(), null);
            return;
          }

          String fullName = DOT_JOINER.join(catalogName, schemaName, modelPO.getModelName());
          modelIdAndNameMap.put(modelPO.getModelId(), fullName);
        });

    return modelIdAndNameMap;
  }

  public static Map<Long, String> getTableObjectFullNames(List<Long> ids) {
    List<TablePO> tablePOs =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class, mapper -> mapper.listTablePOsByTableIds(ids));

    if (tablePOs == null || tablePOs.isEmpty()) {
      return new HashMap<>();
    }

    List<Long> catalogIds =
        tablePOs.stream().map(TablePO::getCatalogId).collect(Collectors.toList());
    List<Long> schemaIds = tablePOs.stream().map(TablePO::getSchemaId).collect(Collectors.toList());

    Map<Long, String> catalogIdAndNameMap = getCatalogIdAndNameMap(catalogIds);
    Map<Long, String> schemaIdAndNameMap = getSchemaIdAndNameMap(schemaIds);

    HashMap<Long, String> tableIdAndNameMap = new HashMap<>();

    tablePOs.forEach(
        tablePO -> {
          // since the catalog or schema can be deleted, we need to check the null value,
          // and when catalog or schema is deleted, we will set fullName of tablePO to null
          String catalogName = catalogIdAndNameMap.getOrDefault(tablePO.getCatalogId(), null);
          if (catalogName == null) {
            LOG.warn("The catalog of table {} may be deleted", tablePO.getTableId());
            tableIdAndNameMap.put(tablePO.getTableId(), null);
            return;
          }

          String schemaName = schemaIdAndNameMap.getOrDefault(tablePO.getSchemaId(), null);
          if (schemaName == null) {
            LOG.warn("The schema of table {} may be deleted", tablePO.getTableId());
            tableIdAndNameMap.put(tablePO.getTableId(), null);
            return;
          }

          String fullName = DOT_JOINER.join(catalogName, schemaName, tablePO.getTableName());
          tableIdAndNameMap.put(tablePO.getTableId(), fullName);
        });

    return tableIdAndNameMap;
  }

  public static Map<Long, String> getTopicObjectFullNames(List<Long> ids) {
    List<TopicPO> topicPOs =
        SessionUtils.getWithoutCommit(
            TopicMetaMapper.class, mapper -> mapper.listTopicPOsByTopicIds(ids));

    if (topicPOs == null || topicPOs.isEmpty()) {
      return new HashMap<>();
    }

    List<Long> catalogIds =
        topicPOs.stream().map(TopicPO::getCatalogId).collect(Collectors.toList());
    List<Long> schemaIds = topicPOs.stream().map(TopicPO::getSchemaId).collect(Collectors.toList());

    Map<Long, String> catalogIdAndNameMap = getCatalogIdAndNameMap(catalogIds);
    Map<Long, String> schemaIdAndNameMap = getSchemaIdAndNameMap(schemaIds);

    HashMap<Long, String> topicIdAndNameMap = new HashMap<>();

    topicPOs.forEach(
        tablePO -> {
          // since the catalog or schema can be deleted, we need to check the null value,
          // and when catalog or schema is deleted, we will set fullName of tablePO to null
          String catalogName = catalogIdAndNameMap.getOrDefault(tablePO.getCatalogId(), null);
          if (catalogName == null) {
            LOG.warn("The catalog of topic {} may be deleted", tablePO.getTopicId());
            topicIdAndNameMap.put(tablePO.getTopicId(), null);
            return;
          }

          String schemaName = schemaIdAndNameMap.getOrDefault(tablePO.getSchemaId(), null);
          if (schemaName == null) {
            LOG.warn("The schema of topic {} may be deleted", tablePO.getTopicId());
            topicIdAndNameMap.put(tablePO.getTopicId(), null);
            return;
          }

          String fullName = DOT_JOINER.join(catalogName, schemaName, tablePO.getTopicName());
          topicIdAndNameMap.put(tablePO.getTopicId(), fullName);
        });

    return topicIdAndNameMap;
  }

  public static Map<Long, String> getColumnObjectFullNames(List<Long> ids) {
    List<ColumnPO> columnPOs =
        SessionUtils.getWithoutCommit(
            TableColumnMapper.class, mapper -> mapper.listColumnPOsByColumnIds(ids));

    if (columnPOs == null || columnPOs.isEmpty()) {
      return new HashMap<>();
    }

    List<Long> catalogIds =
        columnPOs.stream().map(ColumnPO::getCatalogId).collect(Collectors.toList());
    List<Long> schemaIds =
        columnPOs.stream().map(ColumnPO::getSchemaId).collect(Collectors.toList());
    List<Long> tableIds = columnPOs.stream().map(ColumnPO::getTableId).collect(Collectors.toList());

    Map<Long, String> catalogIdAndNameMap = getCatalogIdAndNameMap(catalogIds);
    Map<Long, String> schemaIdAndNameMap = getSchemaIdAndNameMap(schemaIds);
    Map<Long, String> tableIdAndNameMap = getTableIdAndNameMap(tableIds);

    HashMap<Long, String> columnIdAndNameMap = new HashMap<>();

    columnPOs.forEach(
        columnPO -> {
          // since the catalog or schema can be deleted, we need to check the null value,
          // and when catalog or schema is deleted, we will set fullName of filesetPO to null
          String catalogName = catalogIdAndNameMap.getOrDefault(columnPO.getCatalogId(), null);
          if (catalogName == null) {
            LOG.warn("The catalog of column {} may be deleted", columnPO.getColumnId());
            columnIdAndNameMap.put(columnPO.getColumnId(), null);
            return;
          }

          String schemaName = schemaIdAndNameMap.getOrDefault(columnPO.getSchemaId(), null);
          if (schemaName == null) {
            LOG.warn("The schema of column {} may be deleted", columnPO.getColumnId());
            columnIdAndNameMap.put(columnPO.getColumnId(), null);
            return;
          }

          String tableName = tableIdAndNameMap.getOrDefault(columnPO.getTableId(), null);
          if (tableName == null) {
            LOG.warn("The table of column {} may be deleted", columnPO.getColumnId());
            columnIdAndNameMap.put(columnPO.getColumnId(), null);
            return;
          }

          String fullName =
              DOT_JOINER.join(catalogName, schemaName, tableName, columnPO.getColumnName());
          columnIdAndNameMap.put(columnPO.getColumnId(), fullName);
        });

    return columnIdAndNameMap;
  }

  public static Map<Long, String> getCatalogObjectFullNames(List<Long> ids) {
    List<CatalogPO> catalogPOs =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.listCatalogPOsByCatalogIds(ids));

    if (catalogPOs == null || catalogPOs.isEmpty()) {
      return new HashMap<>();
    }

    HashMap<Long, String> catalogIdAndNameMap = new HashMap<>();

    catalogPOs.forEach(
        catalogPO -> {
          if (catalogPO.getCatalogId() == null) {
            catalogIdAndNameMap.put(catalogPO.getCatalogId(), null);
            return;
          }
          catalogIdAndNameMap.put(catalogPO.getCatalogId(), catalogPO.getCatalogName());
        });

    return catalogIdAndNameMap;
  }

  public static Map<Long, String> getCatalogIdAndNameMap(List<Long> catalogIds) {
    List<CatalogPO> catalogPOs =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.listCatalogPOsByCatalogIds(catalogIds));
    return catalogPOs.stream()
        .collect(Collectors.toMap(CatalogPO::getCatalogId, CatalogPO::getCatalogName));
  }

  public static Map<Long, String> getSchemaIdAndNameMap(List<Long> schemaIds) {
    List<SchemaPO> schemaPOS =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class, mapper -> mapper.listSchemaPOsBySchemaIds(schemaIds));
    return schemaPOS.stream()
        .collect(Collectors.toMap(SchemaPO::getSchemaId, SchemaPO::getSchemaName));
  }

  public static Map<Long, String> getTableIdAndNameMap(List<Long> tableIds) {
    List<TablePO> tablePOS =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class, mapper -> mapper.listTablePOsByTableIds(tableIds));
    return tablePOS.stream().collect(Collectors.toMap(TablePO::getTableId, TablePO::getTableName));
  }
}
