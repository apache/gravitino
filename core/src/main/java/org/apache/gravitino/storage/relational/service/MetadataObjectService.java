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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.meta.GenericEntity;
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

  static final Map<MetadataObject.Type, Function<List<Long>, Map<Long, String>>>
      TYPE_TO_FULLNAME_FUNCTION_MAP =
          ImmutableMap.of(
              MetadataObject.Type.METALAKE, MetadataObjectService::getMetalakeObjectsFullName,
              MetadataObject.Type.CATALOG, MetadataObjectService::getCatalogObjectsFullName,
              MetadataObject.Type.SCHEMA, MetadataObjectService::getSchemaObjectsFullName,
              MetadataObject.Type.TABLE, MetadataObjectService::getTableObjectsFullName,
              MetadataObject.Type.FILESET, MetadataObjectService::getFilesetObjectsFullName,
              MetadataObject.Type.MODEL, MetadataObjectService::getModelObjectsFullName,
              MetadataObject.Type.TOPIC, MetadataObjectService::getTopicObjectsFullName,
              MetadataObject.Type.COLUMN, MetadataObjectService::getColumnObjectsFullName);

  private MetadataObjectService() {}

  public static List<MetadataObject> fromGenericEntities(List<GenericEntity> entities) {
    if (entities == null || entities.isEmpty()) {
      return Lists.newArrayList();
    }

    Map<Entity.EntityType, List<Long>> groupIdsByType =
        entities.stream()
            .collect(
                Collectors.groupingBy(
                    GenericEntity::type,
                    Collectors.mapping(GenericEntity::id, Collectors.toList())));

    List<MetadataObject> metadataObjects = Lists.newArrayList();
    for (Map.Entry<Entity.EntityType, List<Long>> entry : groupIdsByType.entrySet()) {
      MetadataObject.Type objectType = MetadataObject.Type.valueOf(entry.getKey().name());
      Map<Long, String> metadataObjectNames =
          TYPE_TO_FULLNAME_FUNCTION_MAP.get(objectType).apply(entry.getValue());

      for (Map.Entry<Long, String> metadataObjectName : metadataObjectNames.entrySet()) {
        String fullName = metadataObjectName.getValue();

        // Metadata object may be deleted asynchronously when we query the name, so it will
        // return null, we should skip this metadata object.
        if (fullName != null) {
          metadataObjects.add(MetadataObjects.parse(fullName, objectType));
        }
      }
    }

    return metadataObjects;
  }

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

  /**
   * Retrieves a map of Metalake object IDs to their full names.
   *
   * @param metalakeIds A list of Metalake object IDs to fetch names for.
   * @return A Map where the key is the Metalake ID and the value is the Metalake full name. The map
   *     may contain null values for the names if its parent object is deleted. Returns an empty map
   *     if no Metalake objects are found for the given IDs. {@code @example} value of metalake full
   *     name: "metalake1.catalog1.schema1.table1"
   */
  public static Map<Long, String> getMetalakeObjectsFullName(List<Long> metalakeIds) {
    List<MetalakePO> metalakePOs =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.listMetalakePOsByMetalakeIds(metalakeIds));

    if (metalakePOs == null || metalakePOs.isEmpty()) {
      return new HashMap<>();
    }

    HashMap<Long, String> metalakeIdAndNameMap = new HashMap<>();

    metalakePOs.forEach(
        metalakePO ->
            metalakeIdAndNameMap.put(metalakePO.getMetalakeId(), metalakePO.getMetalakeName()));

    return metalakeIdAndNameMap;
  }

  /**
   * Retrieves a map of Fileset object IDs to their full names.
   *
   * @param filesetIds A list of Fileset object IDs to fetch names for.
   * @return A Map where the key is the Fileset ID and the value is the Fileset full name. The map
   *     may contain null values for the names if its parent object is deleted. Returns an empty map
   *     if no Fileset objects are found for the given IDs. {@code @example} value of fileset full
   *     name: "catalog1.schema1.fileset1"
   */
  public static Map<Long, String> getFilesetObjectsFullName(List<Long> filesetIds) {
    List<FilesetPO> filesetPOs =
        SessionUtils.getWithoutCommit(
            FilesetMetaMapper.class, mapper -> mapper.listFilesetPOsByFilesetIds(filesetIds));

    if (filesetPOs == null || filesetPOs.isEmpty()) {
      return new HashMap<>();
    }

    List<Long> schemaIds =
        filesetPOs.stream().map(FilesetPO::getSchemaId).collect(Collectors.toList());

    Map<Long, String> schemaIdAndNameMap = getSchemaObjectsFullName(schemaIds);

    HashMap<Long, String> filesetIdAndNameMap = new HashMap<>();

    filesetPOs.forEach(
        filesetPO -> {
          // since the schema can be deleted, we need to check the null value,
          // and when schema is deleted, we will set fullName of filesetPO to null.
          String schemaName = schemaIdAndNameMap.getOrDefault(filesetPO.getSchemaId(), null);
          if (schemaName == null) {
            LOG.warn("The schema of fileset {} may be deleted", filesetPO.getFilesetId());
            filesetIdAndNameMap.put(filesetPO.getFilesetId(), null);
            return;
          }

          String fullName = DOT_JOINER.join(schemaName, filesetPO.getFilesetName());
          filesetIdAndNameMap.put(filesetPO.getFilesetId(), fullName);
        });

    return filesetIdAndNameMap;
  }

  /**
   * Retrieves a map of Model object IDs to their full names.
   *
   * @param modelIds A list of Model object IDs to fetch names for.
   * @return A Map where the key is the Model ID and the value is the Model full name. The map may
   *     contain null values for the names if its parent object is deleted. Returns an empty map if
   *     no Model objects are found for the given IDs. {@code @example} value of model full name:
   *     "catalog1.schema1.model1"
   */
  public static Map<Long, String> getModelObjectsFullName(List<Long> modelIds) {
    List<ModelPO> modelPOs =
        SessionUtils.getWithoutCommit(
            ModelMetaMapper.class, mapper -> mapper.listModelPOsByModelIds(modelIds));

    if (modelPOs == null || modelPOs.isEmpty()) {
      return new HashMap<>();
    }

    List<Long> schemaIds = modelPOs.stream().map(ModelPO::getSchemaId).collect(Collectors.toList());

    Map<Long, String> schemaIdAndNameMap = getSchemaObjectsFullName(schemaIds);

    HashMap<Long, String> modelIdAndNameMap = new HashMap<>();

    modelPOs.forEach(
        modelPO -> {
          // since the schema can be deleted, we need to check the null value,
          // and when schema is deleted, we will set fullName of modelPO to null.
          String schemaName = schemaIdAndNameMap.getOrDefault(modelPO.getSchemaId(), null);
          if (schemaName == null) {
            LOG.warn("The schema of model {} may be deleted", modelPO.getModelId());
            modelIdAndNameMap.put(modelPO.getModelId(), null);
            return;
          }

          String fullName = DOT_JOINER.join(schemaName, modelPO.getModelName());
          modelIdAndNameMap.put(modelPO.getModelId(), fullName);
        });

    return modelIdAndNameMap;
  }

  /**
   * Retrieves a map of Table object IDs to their full names.
   *
   * @param tableIds A list of Table object IDs to fetch names for.
   * @return A Map where the key is the Table ID and the value is the Table full name. The map may
   *     contain null values for the names if its parent object is deleted. Returns an empty map if
   *     no Table objects are found for the given IDs. {@code @example} value of table full name:
   *     "catalog1.schema1.table1"
   */
  public static Map<Long, String> getTableObjectsFullName(List<Long> tableIds) {
    List<TablePO> tablePOs =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class, mapper -> mapper.listTablePOsByTableIds(tableIds));

    if (tablePOs == null || tablePOs.isEmpty()) {
      return new HashMap<>();
    }

    List<Long> schemaIds = tablePOs.stream().map(TablePO::getSchemaId).collect(Collectors.toList());

    Map<Long, String> schemaIdAndNameMap = getSchemaObjectsFullName(schemaIds);

    HashMap<Long, String> tableIdAndNameMap = new HashMap<>();

    tablePOs.forEach(
        tablePO -> {
          // since the schema can be deleted, we need to check the null value,
          // and when schema is deleted, we will set fullName of tablePO to
          // null
          String schemaName = schemaIdAndNameMap.getOrDefault(tablePO.getSchemaId(), null);
          if (schemaName == null) {
            LOG.warn("The schema of table {} may be deleted", tablePO.getTableId());
            tableIdAndNameMap.put(tablePO.getTableId(), null);
            return;
          }

          String fullName = DOT_JOINER.join(schemaName, tablePO.getTableName());
          tableIdAndNameMap.put(tablePO.getTableId(), fullName);
        });

    return tableIdAndNameMap;
  }

  /**
   * Retrieves a map of column object IDs to their full names.
   *
   * @param columnsIds A list of column object IDs to fetch names for.
   * @return A Map where the key is the column ID and the value is the column full name. The map may
   *     contain null values for the names if its parent object is deleted. Returns an empty map if
   *     no column objects are found for the given IDs. {@code @example} value of table full name:
   *     "catalog1.schema1.table1.column1"
   */
  public static Map<Long, String> getColumnObjectsFullName(List<Long> columnsIds) {
    List<ColumnPO> columnPOs =
        SessionUtils.getWithoutCommit(
            TableColumnMapper.class, mapper -> mapper.listColumnPOsByColumnIds(columnsIds));

    if (columnPOs == null || columnPOs.isEmpty()) {
      return new HashMap<>();
    }

    List<Long> tableIds = columnPOs.stream().map(ColumnPO::getTableId).collect(Collectors.toList());
    Map<Long, String> tableIdAndNameMap = getTableObjectsFullName(tableIds);

    HashMap<Long, String> columnIdAndNameMap = new HashMap<>();

    columnPOs.forEach(
        columnPO -> {
          // since the table can be deleted, we need to check the null value,
          // and when the table is deleted, we will set fullName of column to
          // null
          String tableName = tableIdAndNameMap.getOrDefault(columnPO.getTableId(), null);
          if (tableName == null) {
            LOG.warn(
                "The table '{}' of column '{}' may be deleted",
                columnPO.getTableId(),
                columnPO.getColumnId());
            columnIdAndNameMap.put(columnPO.getColumnId(), null);
            return;
          }

          String fullName = DOT_JOINER.join(tableName, columnPO.getColumnName());
          columnIdAndNameMap.put(columnPO.getColumnId(), fullName);
        });

    return columnIdAndNameMap;
  }

  /**
   * Retrieves a map of Topic object IDs to their full names.
   *
   * @param topicIds A list of Topic object IDs to fetch names for.
   * @return A Map where the key is the Topic ID and the value is the Topic full name. The map may
   *     contain null values for the names if its parent object is deleted. Returns an empty map if
   *     no Topic objects are found for the given IDs. {@code @example} value of topic full name:
   *     "catalog1.schema1.topic1"
   */
  public static Map<Long, String> getTopicObjectsFullName(List<Long> topicIds) {
    List<TopicPO> topicPOs =
        SessionUtils.getWithoutCommit(
            TopicMetaMapper.class, mapper -> mapper.listTopicPOsByTopicIds(topicIds));

    if (topicPOs == null || topicPOs.isEmpty()) {
      return new HashMap<>();
    }

    List<Long> schemaIds = topicPOs.stream().map(TopicPO::getSchemaId).collect(Collectors.toList());

    Map<Long, String> schemaIdAndNameMap = getSchemaObjectsFullName(schemaIds);

    HashMap<Long, String> topicIdAndNameMap = new HashMap<>();

    topicPOs.forEach(
        topicPO -> {
          // since the schema can be deleted, we need to check the null value,
          // and when schema is deleted, we will set fullName of topicPO to null.
          String schemaName = schemaIdAndNameMap.getOrDefault(topicPO.getSchemaId(), null);
          if (schemaName == null) {
            LOG.warn("The schema of topic {} may be deleted", topicPO.getTopicId());
            topicIdAndNameMap.put(topicPO.getTopicId(), null);
            return;
          }

          String fullName = DOT_JOINER.join(schemaName, topicPO.getTopicName());
          topicIdAndNameMap.put(topicPO.getTopicId(), fullName);
        });

    return topicIdAndNameMap;
  }

  /**
   * Retrieves a map of Catalog object IDs to their full names.
   *
   * @param catalogIds A list of Catalog object IDs to fetch names for.
   * @return A Map where the key is the Catalog ID and the value is the Catalog full name. The map
   *     may contain null values for the names if its parent object is deleted. Returns an empty map
   *     if no Catalog objects are found for the given IDs. {@code @example} value of catalog full
   *     name: "catalog1"
   */
  public static Map<Long, String> getCatalogObjectsFullName(List<Long> catalogIds) {
    List<CatalogPO> catalogPOs =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.listCatalogPOsByCatalogIds(catalogIds));

    if (catalogPOs == null || catalogPOs.isEmpty()) {
      return new HashMap<>();
    }

    HashMap<Long, String> catalogIdAndNameMap = new HashMap<>();

    catalogPOs.forEach(
        catalogPO -> catalogIdAndNameMap.put(catalogPO.getCatalogId(), catalogPO.getCatalogName()));

    return catalogIdAndNameMap;
  }

  /**
   * Retrieves a map of Schema object IDs to their full names.
   *
   * @param schemaIds A list of Schema object IDs to fetch names for.
   * @return A Map where the key is the Schema ID and the value is the Schema full name. The map may
   *     contain null values for the names if its parent object is deleted. Returns an empty map if
   *     no Schema objects are found for the given IDs. {@code @example} value of schema full name:
   *     "catalog1.schema1"
   */
  public static Map<Long, String> getSchemaObjectsFullName(List<Long> schemaIds) {
    List<SchemaPO> schemaPOs =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class, mapper -> mapper.listSchemaPOsBySchemaIds(schemaIds));

    if (schemaPOs == null || schemaPOs.isEmpty()) {
      return new HashMap<>();
    }

    List<Long> catalogIds =
        schemaPOs.stream().map(SchemaPO::getCatalogId).collect(Collectors.toList());

    Map<Long, String> catalogIdAndNameMap = getCatalogObjectsFullName(catalogIds);

    HashMap<Long, String> schemaIdAndNameMap = new HashMap<>();

    schemaPOs.forEach(
        schemaPO -> {
          String catalogName = catalogIdAndNameMap.getOrDefault(schemaPO.getCatalogId(), null);
          if (catalogName == null) {
            LOG.warn("The catalog of schema {} may be deleted", schemaPO.getSchemaId());
            schemaIdAndNameMap.put(schemaPO.getSchemaId(), null);
            return;
          }

          String fullName = DOT_JOINER.join(catalogName, schemaPO.getSchemaName());

          schemaIdAndNameMap.put(schemaPO.getSchemaId(), fullName);
        });

    return schemaIdAndNameMap;
  }
}
