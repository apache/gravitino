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
package org.apache.gravitino.connector;

import static org.apache.gravitino.file.Fileset.PROPERTY_DEFAULT_LOCATION_NAME;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.TestColumn;
import org.apache.gravitino.TestFileset;
import org.apache.gravitino.TestModel;
import org.apache.gravitino.TestModelVersion;
import org.apache.gravitino.TestSchema;
import org.apache.gravitino.TestTable;
import org.apache.gravitino.TestTopic;
import org.apache.gravitino.audit.CallerContext;
import org.apache.gravitino.audit.FilesetAuditConstants;
import org.apache.gravitino.audit.FilesetDataOperation;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchLocationNameException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchModelVersionURINameException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NoSuchTopicException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.exceptions.TopicAlreadyExistsException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.messaging.DataLayout;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.messaging.TopicCatalog;
import org.apache.gravitino.messaging.TopicChange;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelCatalog;
import org.apache.gravitino.model.ModelChange;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.model.ModelVersionChange;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCatalogOperations
    implements CatalogOperations,
        TableCatalog,
        FilesetCatalog,
        TopicCatalog,
        ModelCatalog,
        SupportsSchemas {
  private static final Logger LOG = LoggerFactory.getLogger(TestCatalogOperations.class);

  private final Map<NameIdentifier, TestTable> tables;

  private final Map<NameIdentifier, TestSchema> schemas;

  private final Map<NameIdentifier, TestFileset> filesets;

  private final Map<NameIdentifier, TestTopic> topics;

  private final Map<NameIdentifier, TestModel> models;

  private final Map<Pair<NameIdentifier, Integer>, TestModelVersion> modelVersions;

  private final Map<Pair<NameIdentifier, String>, Integer> modelAliasToVersion;

  public static final String FAIL_CREATE = "fail-create";

  public static final String FAIL_TEST = "need-fail";

  private static final String SLASH = "/";

  public TestCatalogOperations(Map<String, String> config) {
    tables = Maps.newHashMap();
    schemas = Maps.newHashMap();
    filesets = Maps.newHashMap();
    topics = Maps.newHashMap();
    models = Maps.newHashMap();
    modelVersions = Maps.newHashMap();
    modelAliasToVersion = Maps.newHashMap();
  }

  @Override
  public void initialize(
      Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertyMetadata)
      throws RuntimeException {}

  @Override
  public void close() throws IOException {}

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    return tables.keySet().stream()
        .filter(testTable -> testTable.namespace().equals(namespace))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    if (tables.containsKey(ident)) {
      return tables.get(ident);
    } else {
      throw new NoSuchTableException("Table %s does not exist", ident);
    }
  }

  @Override
  public Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

    TestColumn[] sortedColumns =
        IntStream.range(0, columns.length)
            .mapToObj(
                i ->
                    TestColumn.builder()
                        .withName(columns[i].name())
                        .withPosition(i)
                        .withComment(columns[i].comment())
                        .withType(columns[i].dataType())
                        .withNullable(columns[i].nullable())
                        .withAutoIncrement(columns[i].autoIncrement())
                        .withDefaultValue(columns[i].defaultValue())
                        .build())
            .sorted(Comparator.comparingInt(TestColumn::position))
            .toArray(TestColumn[]::new);

    TestTable table =
        TestTable.builder()
            .withName(ident.name())
            .withComment(comment)
            .withProperties(new HashMap<>(properties))
            .withAuditInfo(auditInfo)
            .withColumns(sortedColumns)
            .withDistribution(distribution)
            .withSortOrders(sortOrders)
            .withPartitioning(partitions)
            .withIndexes(indexes)
            .build();

    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException("Table %s already exists", ident);
    } else {
      tables.put(ident, table);
    }

    return TestTable.builder()
        .withName(ident.name())
        .withComment(comment)
        .withProperties(new HashMap<>(properties))
        .withAuditInfo(auditInfo)
        .withColumns(sortedColumns)
        .withDistribution(distribution)
        .withSortOrders(sortOrders)
        .withPartitioning(partitions)
        .withIndexes(indexes)
        .build();
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    if (!tables.containsKey(ident)) {
      throw new NoSuchTableException("Table %s does not exist", ident);
    }

    AuditInfo updatedAuditInfo =
        AuditInfo.builder()
            .withCreator("test")
            .withCreateTime(Instant.now())
            .withLastModifier("test")
            .withLastModifiedTime(Instant.now())
            .build();

    TestTable table = tables.get(ident);
    Map<String, String> newProps =
        table.properties() != null ? Maps.newHashMap(table.properties()) : Maps.newHashMap();

    NameIdentifier newIdent = ident;
    for (TableChange change : changes) {
      if (change instanceof TableChange.SetProperty) {
        newProps.put(
            ((TableChange.SetProperty) change).getProperty(),
            ((TableChange.SetProperty) change).getValue());
      } else if (change instanceof TableChange.RemoveProperty) {
        newProps.remove(((TableChange.RemoveProperty) change).getProperty());
      } else if (change instanceof TableChange.RenameTable) {
        String newName = ((TableChange.RenameTable) change).getNewName();
        newIdent = NameIdentifier.of(ident.namespace(), newName);
        if (tables.containsKey(newIdent)) {
          throw new TableAlreadyExistsException("Table %s already exists", ident);
        }
      } else {
        // do nothing
      }
    }

    TableChange.ColumnChange[] columnChanges =
        Arrays.stream(changes)
            .filter(change -> change instanceof TableChange.ColumnChange)
            .map(change -> (TableChange.ColumnChange) change)
            .toArray(TableChange.ColumnChange[]::new);
    Column[] newColumns = updateColumns(table.columns(), columnChanges);

    TestTable updatedTable =
        TestTable.builder()
            .withName(newIdent.name())
            .withComment(table.comment())
            .withProperties(new HashMap<>(newProps))
            .withAuditInfo(updatedAuditInfo)
            .withColumns(newColumns)
            .withPartitioning(table.partitioning())
            .withDistribution(table.distribution())
            .withSortOrders(table.sortOrder())
            .withIndexes(table.index())
            .build();

    tables.put(ident, updatedTable);
    return updatedTable;
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    if (tables.containsKey(ident)) {
      tables.remove(ident);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    return schemas.keySet().stream()
        .filter(ident -> ident.namespace().equals(namespace))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

    TestSchema schema =
        TestSchema.builder()
            .withName(ident.name())
            .withComment(comment)
            .withProperties(properties)
            .withAuditInfo(auditInfo)
            .build();

    if (schemas.containsKey(ident)) {
      throw new SchemaAlreadyExistsException("Schema %s already exists", ident);
    } else {
      schemas.put(ident, schema);
    }

    return schema;
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    if (schemas.containsKey(ident)) {
      return schemas.get(ident);
    } else {
      throw new NoSuchSchemaException("Schema %s does not exist", ident);
    }
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    if (!schemas.containsKey(ident)) {
      throw new NoSuchSchemaException("Schema %s does not exist", ident);
    }

    AuditInfo updatedAuditInfo =
        AuditInfo.builder()
            .withCreator("test")
            .withCreateTime(Instant.now())
            .withLastModifier("test")
            .withLastModifiedTime(Instant.now())
            .build();

    TestSchema schema = schemas.get(ident);
    Map<String, String> newProps =
        schema.properties() != null ? Maps.newHashMap(schema.properties()) : Maps.newHashMap();

    for (SchemaChange change : changes) {
      if (change instanceof SchemaChange.SetProperty) {
        newProps.put(
            ((SchemaChange.SetProperty) change).getProperty(),
            ((SchemaChange.SetProperty) change).getValue());
      } else if (change instanceof SchemaChange.RemoveProperty) {
        newProps.remove(((SchemaChange.RemoveProperty) change).getProperty());
      } else {
        throw new IllegalArgumentException("Unsupported schema change: " + change);
      }
    }

    TestSchema updatedSchema =
        TestSchema.builder()
            .withName(ident.name())
            .withComment(schema.comment())
            .withProperties(newProps)
            .withAuditInfo(updatedAuditInfo)
            .build();

    schemas.put(ident, updatedSchema);
    return updatedSchema;
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    if (!schemas.containsKey(ident)) {
      return false;
    }

    schemas.remove(ident);
    if (cascade) {
      tables.keySet().stream()
          .filter(table -> table.namespace().toString().equals(ident.toString()))
          .forEach(tables::remove);
    }

    return true;
  }

  @Override
  public NameIdentifier[] listFilesets(Namespace namespace) throws NoSuchSchemaException {
    return filesets.keySet().stream()
        .filter(ident -> ident.namespace().equals(namespace))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    if (filesets.containsKey(ident)) {
      return filesets.get(ident);
    } else {
      throw new NoSuchFilesetException("Fileset %s does not exist", ident);
    }
  }

  @Override
  public Fileset createMultipleLocationFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      Map<String, String> storageLocations,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    if (storageLocations != null && storageLocations.size() == 1) {
      properties =
          Optional.ofNullable(properties)
              .map(
                  props ->
                      ImmutableMap.<String, String>builder()
                          .putAll(props)
                          .put(
                              PROPERTY_DEFAULT_LOCATION_NAME,
                              storageLocations.keySet().iterator().next())
                          .build())
              .orElseGet(
                  () ->
                      ImmutableMap.of(
                          PROPERTY_DEFAULT_LOCATION_NAME,
                          storageLocations.keySet().iterator().next()));
    }
    TestFileset fileset =
        TestFileset.builder()
            .withName(ident.name())
            .withComment(comment)
            .withProperties(properties)
            .withAuditInfo(auditInfo)
            .withType(type)
            .withStorageLocations(storageLocations)
            .build();

    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    if (filesets.containsKey(ident)) {
      throw new FilesetAlreadyExistsException("Fileset %s already exists", ident);
    } else if (!schemas.containsKey(schemaIdent)) {
      throw new NoSuchSchemaException("Schema %s does not exist", schemaIdent);
    } else {
      filesets.put(ident, fileset);
    }

    return fileset;
  }

  @Override
  public Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, IllegalArgumentException {
    if (!filesets.containsKey(ident)) {
      throw new NoSuchFilesetException("Fileset %s does not exist", ident);
    }

    AuditInfo updatedAuditInfo =
        AuditInfo.builder()
            .withCreator("test")
            .withCreateTime(Instant.now())
            .withLastModifier("test")
            .withLastModifiedTime(Instant.now())
            .build();

    TestFileset fileset = filesets.get(ident);
    Map<String, String> newProps =
        fileset.properties() != null ? Maps.newHashMap(fileset.properties()) : Maps.newHashMap();
    NameIdentifier newIdent = ident;
    String newComment = fileset.comment();

    for (FilesetChange change : changes) {
      if (change instanceof FilesetChange.SetProperty) {
        newProps.put(
            ((FilesetChange.SetProperty) change).getProperty(),
            ((FilesetChange.SetProperty) change).getValue());
      } else if (change instanceof FilesetChange.RemoveProperty) {
        newProps.remove(((FilesetChange.RemoveProperty) change).getProperty());
      } else if (change instanceof FilesetChange.RenameFileset) {
        String newName = ((FilesetChange.RenameFileset) change).getNewName();
        newIdent = NameIdentifier.of(ident.namespace(), newName);
        if (filesets.containsKey(newIdent)) {
          throw new FilesetAlreadyExistsException("Fileset %s already exists", ident);
        }
        filesets.remove(ident);
      } else if (change instanceof FilesetChange.UpdateFilesetComment) {
        newComment = ((FilesetChange.UpdateFilesetComment) change).getNewComment();
      } else if (change instanceof FilesetChange.RemoveComment) {
        newComment = null;
      } else {
        throw new IllegalArgumentException("Unsupported fileset change: " + change);
      }
    }

    TestFileset updatedFileset =
        TestFileset.builder()
            .withName(newIdent.name())
            .withComment(newComment)
            .withProperties(newProps)
            .withAuditInfo(updatedAuditInfo)
            .withType(fileset.type())
            .withStorageLocations(fileset.storageLocations())
            .build();
    filesets.put(newIdent, updatedFileset);
    return updatedFileset;
  }

  @Override
  public boolean dropFileset(NameIdentifier ident) {
    if (filesets.containsKey(ident)) {
      filesets.remove(ident);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public String getFileLocation(NameIdentifier ident, String subPath, String locationName) {
    Preconditions.checkArgument(subPath != null, "subPath must not be null");
    String processedSubPath;
    if (!subPath.trim().isEmpty() && !subPath.trim().startsWith(SLASH)) {
      processedSubPath = SLASH + subPath.trim();
    } else {
      processedSubPath = subPath.trim();
    }

    Fileset fileset = loadFileset(ident);
    Map<String, String> storageLocations = fileset.storageLocations();
    String targetLocationName =
        Optional.ofNullable(locationName)
            .orElse(fileset.properties().get(PROPERTY_DEFAULT_LOCATION_NAME));
    if (storageLocations == null || !storageLocations.containsKey(targetLocationName)) {
      throw new NoSuchLocationNameException(
          "The location name: %s does not exist in the fileset: %s", targetLocationName, ident);
    }

    boolean isSingleFile = checkSingleFile(storageLocations.get(targetLocationName));
    // if the storage location is a single file, it cannot have sub path to access.
    if (isSingleFile && StringUtils.isBlank(processedSubPath)) {
      throw new GravitinoRuntimeException(
          "Sub path should always be blank, because the fileset only mounts a single file.");
    }

    // do checks for some data operations.
    if (hasCallerContext()) {
      Map<String, String> contextMap = CallerContext.CallerContextHolder.get().context();
      String operation =
          contextMap.getOrDefault(
              FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION,
              FilesetDataOperation.UNKNOWN.name());
      if (!FilesetDataOperation.checkValid(operation)) {
        LOG.warn(
            "The data operation: {} is not valid, we cannot do some checks for this operation.",
            operation);
      } else {
        FilesetDataOperation dataOperation = FilesetDataOperation.valueOf(operation);
        switch (dataOperation) {
          case RENAME:
            // Fileset only mounts a single file, the storage location of the fileset cannot be
            // renamed; Otherwise the metadata in the Gravitino server may be inconsistent.
            if (isSingleFile) {
              throw new GravitinoRuntimeException(
                  "Cannot rename the fileset: %s which only mounts to a single file.", ident);
            }
            // if the sub path is blank, it cannot be renamed,
            // otherwise the metadata in the Gravitino server may be inconsistent.
            if (StringUtils.isBlank(processedSubPath)
                || (processedSubPath.startsWith(SLASH) && processedSubPath.length() == 1)) {
              throw new GravitinoRuntimeException(
                  "subPath cannot be blank when need to rename a file or a directory.");
            }
            break;
          default:
            break;
        }
      }
    }

    String fileLocation;
    // 1. if the storage location is a single file, we pass the storage location directly
    // 2. if the processed sub path is blank, we pass the storage location directly
    if (isSingleFile || StringUtils.isBlank(processedSubPath)) {
      fileLocation = fileset.storageLocation();
    } else {
      // the processed sub path always starts with "/" if it is not blank,
      // so we can safely remove the tailing slash if storage location ends with "/".
      String storageLocation =
          fileset.storageLocation().endsWith(SLASH)
              ? fileset.storageLocation().substring(0, fileset.storageLocation().length() - 1)
              : fileset.storageLocation();
      fileLocation = String.format("%s%s", storageLocation, processedSubPath);
    }
    return fileLocation;
  }

  @Override
  public NameIdentifier[] listTopics(Namespace namespace) throws NoSuchSchemaException {
    return topics.keySet().stream()
        .filter(ident -> ident.namespace().equals(namespace))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Topic loadTopic(NameIdentifier ident) throws NoSuchTopicException {
    if (topics.containsKey(ident)) {
      return topics.get(ident);
    } else {
      throw new NoSuchTopicException("Topic %s does not exist", ident);
    }
  }

  @Override
  public Topic createTopic(
      NameIdentifier ident, String comment, DataLayout dataLayout, Map<String, String> properties)
      throws NoSuchSchemaException, TopicAlreadyExistsException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    TestTopic topic =
        TestTopic.builder()
            .withName(ident.name())
            .withComment(comment)
            .withProperties(properties)
            .withAuditInfo(auditInfo)
            .build();

    if (topics.containsKey(ident)) {
      throw new TopicAlreadyExistsException("Topic %s already exists", ident);
    } else {
      topics.put(ident, topic);
    }

    return topic;
  }

  @Override
  public Topic alterTopic(NameIdentifier ident, TopicChange... changes)
      throws NoSuchTopicException, IllegalArgumentException {
    if (!topics.containsKey(ident)) {
      throw new NoSuchTopicException("Topic %s does not exist", ident);
    }

    AuditInfo updatedAuditInfo =
        AuditInfo.builder()
            .withCreator("test")
            .withCreateTime(Instant.now())
            .withLastModifier("test")
            .withLastModifiedTime(Instant.now())
            .build();

    TestTopic topic = topics.get(ident);
    Map<String, String> newProps =
        topic.properties() != null ? Maps.newHashMap(topic.properties()) : Maps.newHashMap();
    String newComment = topic.comment();

    for (TopicChange change : changes) {
      if (change instanceof TopicChange.SetProperty) {
        newProps.put(
            ((TopicChange.SetProperty) change).getProperty(),
            ((TopicChange.SetProperty) change).getValue());
      } else if (change instanceof TopicChange.RemoveProperty) {
        newProps.remove(((TopicChange.RemoveProperty) change).getProperty());
      } else if (change instanceof TopicChange.UpdateTopicComment) {
        newComment = ((TopicChange.UpdateTopicComment) change).getNewComment();
      } else {
        throw new IllegalArgumentException("Unsupported topic change: " + change);
      }
    }

    TestTopic updatedTopic =
        TestTopic.builder()
            .withName(ident.name())
            .withComment(newComment)
            .withProperties(newProps)
            .withAuditInfo(updatedAuditInfo)
            .build();

    topics.put(ident, updatedTopic);
    return updatedTopic;
  }

  @Override
  public boolean dropTopic(NameIdentifier ident) throws NoSuchTopicException {
    if (topics.containsKey(ident)) {
      topics.remove(ident);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void testConnection(
      NameIdentifier name,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties) {
    if ("true".equals(properties.get(FAIL_TEST))) {
      throw new ConnectionFailedException("Connection failed");
    }
  }

  @Override
  public NameIdentifier[] listModels(Namespace namespace) throws NoSuchSchemaException {
    NameIdentifier modelSchemaIdent = NameIdentifier.of(namespace.levels());
    if (!schemas.containsKey(modelSchemaIdent)) {
      throw new NoSuchSchemaException("Schema %s does not exist", modelSchemaIdent);
    }

    return models.keySet().stream()
        .filter(ident -> ident.namespace().equals(namespace))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Model getModel(NameIdentifier ident) throws NoSuchModelException {
    if (models.containsKey(ident)) {
      return models.get(ident);
    } else {
      throw new NoSuchModelException("Model %s does not exist", ident);
    }
  }

  @Override
  public Model registerModel(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchSchemaException, ModelAlreadyExistsException {
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    if (!schemas.containsKey(schemaIdent)) {
      throw new NoSuchSchemaException("Schema %s does not exist", schemaIdent);
    }

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    TestModel model =
        TestModel.builder()
            .withName(ident.name())
            .withComment(comment)
            .withProperties(properties)
            .withLatestVersion(0)
            .withAuditInfo(auditInfo)
            .build();

    if (models.containsKey(ident)) {
      throw new ModelAlreadyExistsException("Model %s already exists", ident);
    } else {
      models.put(ident, model);
    }

    return model;
  }

  @Override
  public boolean deleteModel(NameIdentifier ident) {
    if (!models.containsKey(ident)) {
      return false;
    }

    models.remove(ident);

    List<Pair<NameIdentifier, Integer>> deletedVersions =
        modelVersions.entrySet().stream()
            .filter(e -> e.getKey().getLeft().equals(ident))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    deletedVersions.forEach(modelVersions::remove);

    List<Pair<NameIdentifier, String>> deletedAliases =
        modelAliasToVersion.entrySet().stream()
            .filter(e -> e.getKey().getLeft().equals(ident))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    deletedAliases.forEach(modelAliasToVersion::remove);

    return true;
  }

  @Override
  public int[] listModelVersions(NameIdentifier ident) throws NoSuchModelException {
    if (!models.containsKey(ident)) {
      throw new NoSuchModelException("Model %s does not exist", ident);
    }

    return modelVersions.entrySet().stream()
        .filter(e -> e.getKey().getLeft().equals(ident))
        .mapToInt(e -> e.getValue().version())
        .toArray();
  }

  @Override
  public ModelVersion[] listModelVersionInfos(NameIdentifier ident) throws NoSuchModelException {
    if (!models.containsKey(ident)) {
      throw new NoSuchModelException("Model %s does not exist", ident);
    }

    return modelVersions.entrySet().stream()
        .filter(e -> e.getKey().getLeft().equals(ident))
        .map(e -> e.getValue())
        .toArray(ModelVersion[]::new);
  }

  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, int version)
      throws NoSuchModelVersionException {
    if (!models.containsKey(ident)) {
      throw new NoSuchModelVersionException("Model %s does not exist", ident);
    }

    Pair<NameIdentifier, Integer> versionPair = Pair.of(ident, version);
    if (!modelVersions.containsKey(versionPair)) {
      throw new NoSuchModelVersionException("Model version %s does not exist", versionPair);
    }

    return modelVersions.get(versionPair);
  }

  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, String alias)
      throws NoSuchModelVersionException {
    if (!models.containsKey(ident)) {
      throw new NoSuchModelVersionException("Model %s does not exist", ident);
    }

    Pair<NameIdentifier, String> aliasPair = Pair.of(ident, alias);
    if (!modelAliasToVersion.containsKey(aliasPair)) {
      throw new NoSuchModelVersionException("Model version %s does not exist", alias);
    }

    int version = modelAliasToVersion.get(aliasPair);
    Pair<NameIdentifier, Integer> versionPair = Pair.of(ident, version);
    if (!modelVersions.containsKey(versionPair)) {
      throw new NoSuchModelVersionException("Model version %s does not exist", versionPair);
    }

    return modelVersions.get(versionPair);
  }

  @Override
  public void linkModelVersion(
      NameIdentifier ident,
      Map<String, String> uris,
      String[] aliases,
      String comment,
      Map<String, String> properties)
      throws NoSuchModelException, ModelVersionAliasesAlreadyExistException {
    if (!models.containsKey(ident)) {
      throw new NoSuchModelException("Model %s does not exist", ident);
    }

    if (uris == null || uris.isEmpty()) {
      throw new IllegalArgumentException("Uri must be set for model version");
    }
    uris.forEach(
        (name, uri) -> {
          if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("URI name must not be blank");
          }
          if (StringUtils.isBlank(uri)) {
            throw new IllegalArgumentException("URI must not be blank for name: " + name);
          }
        });

    String[] aliasArray = aliases != null ? aliases : new String[0];
    for (String alias : aliasArray) {
      Pair<NameIdentifier, String> aliasPair = Pair.of(ident, alias);
      if (modelAliasToVersion.containsKey(aliasPair)) {
        throw new ModelVersionAliasesAlreadyExistException(
            "Model version alias %s already exists", alias);
      }
    }

    int version = models.get(ident).latestVersion();
    TestModelVersion modelVersion =
        TestModelVersion.builder()
            .withVersion(version)
            .withAliases(aliases)
            .withComment(comment)
            .withUris(uris)
            .withProperties(properties)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    Pair<NameIdentifier, Integer> versionPair = Pair.of(ident, version);
    modelVersions.put(versionPair, modelVersion);
    for (String alias : aliasArray) {
      Pair<NameIdentifier, String> aliasPair = Pair.of(ident, alias);
      modelAliasToVersion.put(aliasPair, version);
    }

    TestModel model = models.get(ident);
    TestModel updatedModel =
        TestModel.builder()
            .withName(model.name())
            .withComment(model.comment())
            .withProperties(model.properties())
            .withLatestVersion(version + 1)
            .withAuditInfo(model.auditInfo())
            .build();
    models.put(ident, updatedModel);
  }

  @Override
  public String getModelVersionUri(NameIdentifier ident, String alias, String uriName)
      throws NoSuchModelVersionException, NoSuchModelVersionURINameException {
    Model model = getModel(ident);
    ModelVersion modelVersion = getModelVersion(ident, alias);
    return internalGetModelVersionUri(model, modelVersion, uriName);
  }

  @Override
  public String getModelVersionUri(NameIdentifier ident, int version, String uriName)
      throws NoSuchModelVersionException, NoSuchModelVersionURINameException {
    Model model = getModel(ident);
    ModelVersion modelVersion = getModelVersion(ident, version);
    return internalGetModelVersionUri(model, modelVersion, uriName);
  }

  @Override
  public boolean deleteModelVersion(NameIdentifier ident, int version) {
    if (!models.containsKey(ident)) {
      return false;
    }

    Pair<NameIdentifier, Integer> versionPair = Pair.of(ident, version);
    if (!modelVersions.containsKey(versionPair)) {
      return false;
    }

    TestModelVersion modelVersion = modelVersions.remove(versionPair);
    if (modelVersion.aliases() != null) {
      for (String alias : modelVersion.aliases()) {
        Pair<NameIdentifier, String> aliasPair = Pair.of(ident, alias);
        modelAliasToVersion.remove(aliasPair);
      }
    }

    return true;
  }

  @Override
  public boolean deleteModelVersion(NameIdentifier ident, String alias) {
    if (!models.containsKey(ident)) {
      return false;
    }

    Pair<NameIdentifier, String> aliasPair = Pair.of(ident, alias);
    if (!modelAliasToVersion.containsKey(aliasPair)) {
      return false;
    }

    int version = modelAliasToVersion.remove(aliasPair);
    Pair<NameIdentifier, Integer> versionPair = Pair.of(ident, version);
    if (!modelVersions.containsKey(versionPair)) {
      return false;
    }

    TestModelVersion modelVersion = modelVersions.remove(versionPair);
    for (String modelVersionAlias : modelVersion.aliases()) {
      Pair<NameIdentifier, String> modelAliasPair = Pair.of(ident, modelVersionAlias);
      modelAliasToVersion.remove(modelAliasPair);
    }

    return true;
  }

  @Override
  public Model alterModel(NameIdentifier ident, ModelChange... changes)
      throws NoSuchModelException, IllegalArgumentException {
    if (!models.containsKey(ident)) {
      throw new NoSuchModelException("Model %s does not exist", ident);
    }

    AuditInfo updatedAuditInfo =
        AuditInfo.builder()
            .withCreator("test")
            .withCreateTime(Instant.now())
            .withLastModifier("test")
            .withLastModifiedTime(Instant.now())
            .build();

    TestModel model = models.get(ident);
    Map<String, String> newProps =
        model.properties() == null ? ImmutableMap.of() : new HashMap<>(model.properties());
    String newComment = model.comment();
    int newLatestVersion = model.latestVersion();

    NameIdentifier newIdent = ident;
    for (ModelChange change : changes) {
      if (change instanceof ModelChange.RenameModel) {
        String newName = ((ModelChange.RenameModel) change).newName();
        newIdent = NameIdentifier.of(ident.namespace(), newName);
        if (models.containsKey(newIdent)) {
          throw new ModelAlreadyExistsException("Model %s already exists", ident);
        }

      } else if (change instanceof ModelChange.RemoveProperty) {
        ModelChange.RemoveProperty removeProperty = (ModelChange.RemoveProperty) change;
        newProps.remove(removeProperty.property());

      } else if (change instanceof ModelChange.SetProperty) {
        ModelChange.SetProperty setProperty = (ModelChange.SetProperty) change;
        newProps.put(setProperty.property(), setProperty.value());

      } else if (change instanceof ModelChange.UpdateComment) {
        ModelChange.UpdateComment updateComment = (ModelChange.UpdateComment) change;
        newComment = updateComment.newComment();

      } else {
        throw new IllegalArgumentException("Unsupported model change: " + change);
      }
    }
    TestModel updatedModel =
        TestModel.builder()
            .withName(newIdent.name())
            .withComment(newComment)
            .withProperties(new HashMap<>(newProps))
            .withAuditInfo(updatedAuditInfo)
            .withLatestVersion(newLatestVersion)
            .build();

    models.put(ident, updatedModel);
    return updatedModel;
  }

  /** {@inheritDoc} */
  @Override
  public ModelVersion alterModelVersion(
      NameIdentifier ident, int version, ModelVersionChange... changes)
      throws NoSuchModelException, NoSuchModelVersionException, IllegalArgumentException {

    if (!models.containsKey(ident)) {
      throw new NoSuchModelVersionException("Model %s does not exist", ident);
    }

    Pair<NameIdentifier, Integer> versionPair = Pair.of(ident, version);
    if (!modelVersions.containsKey(versionPair)) {
      throw new NoSuchModelVersionException("Model version %s does not exist", versionPair);
    }

    return internalUpdateModelVersion(ident, version, changes);
  }

  /** {@inheritDoc} */
  @Override
  public ModelVersion alterModelVersion(
      NameIdentifier ident, String alias, ModelVersionChange... changes)
      throws NoSuchModelException, IllegalArgumentException {

    if (!models.containsKey(ident)) {
      throw new NoSuchModelVersionException("Model %s does not exist", ident);
    }

    Pair<NameIdentifier, String> aliasPair = Pair.of(ident, alias);
    if (!modelAliasToVersion.containsKey(aliasPair)) {
      throw new NoSuchModelVersionException("Model version %s does not exist", alias);
    }

    int version = modelAliasToVersion.get(aliasPair);
    Pair<NameIdentifier, Integer> versionPair = Pair.of(ident, version);
    if (!modelVersions.containsKey(versionPair)) {
      throw new NoSuchModelVersionException("Model version %s does not exist", versionPair);
    }

    return internalUpdateModelVersion(ident, version, changes);
  }

  private String internalGetModelVersionUri(
      Model model, ModelVersion modelVersion, String uriName) {
    Map<String, String> uris = modelVersion.uris();
    // If the uriName is not null, get from the uris directly
    if (uriName != null) {
      return getUriByName(uris, uriName);
    }

    // If there is only one uri of the model version, use it
    if (uris.size() == 1) {
      return uris.values().iterator().next();
    }

    // If the uri name is null, try to get the default uri name from the model version properties
    Map<String, String> modelVersionProperties = modelVersion.properties();
    if (modelVersionProperties.containsKey(ModelVersion.PROPERTY_DEFAULT_URI_NAME)) {
      String defaultUriName = modelVersionProperties.get(ModelVersion.PROPERTY_DEFAULT_URI_NAME);
      return getUriByName(uris, defaultUriName);
    }

    // If the default uri name is not set for the model version, try to get the default uri name
    // from the model properties
    Map<String, String> modelProperties = model.properties();
    if (modelProperties.containsKey(ModelVersion.PROPERTY_DEFAULT_URI_NAME)) {
      String defaultUriName = modelProperties.get(ModelVersion.PROPERTY_DEFAULT_URI_NAME);
      return getUriByName(uris, defaultUriName);
    }

    throw new IllegalArgumentException("Either uri name of default uri name should be provided");
  }

  private String getUriByName(Map<String, String> uris, String uriName) {
    if (!uris.containsKey(uriName)) {
      throw new NoSuchModelVersionURINameException("URI name %s does not exist", uriName);
    }
    return uris.get(uriName);
  }

  private ModelVersion internalUpdateModelVersion(
      NameIdentifier ident, int version, ModelVersionChange... changes)
      throws NoSuchModelException, NoSuchModelVersionException, IllegalArgumentException {

    Pair<NameIdentifier, Integer> versionPair = Pair.of(ident, version);
    AuditInfo updatedAuditInfo =
        AuditInfo.builder()
            .withCreator("test")
            .withCreateTime(Instant.now())
            .withLastModifier("test")
            .withLastModifiedTime(Instant.now())
            .build();

    TestModelVersion testModelVersion = modelVersions.get(versionPair);
    Map<String, String> newProps =
        testModelVersion.properties() != null
            ? Maps.newHashMap(testModelVersion.properties())
            : Maps.newHashMap();
    String newComment = testModelVersion.comment();
    int newVersion = testModelVersion.version();
    String[] newAliases = testModelVersion.aliases();
    Map<String, String> newUris = Maps.newHashMap(testModelVersion.uris());

    for (ModelVersionChange change : changes) {
      if (change instanceof ModelVersionChange.UpdateComment) {
        newComment = ((ModelVersionChange.UpdateComment) change).newComment();

      } else if (change instanceof ModelVersionChange.RemoveProperty) {
        ModelVersionChange.RemoveProperty removeProperty =
            (ModelVersionChange.RemoveProperty) change;
        newProps.remove(removeProperty.property());

      } else if (change instanceof ModelVersionChange.SetProperty) {
        ModelVersionChange.SetProperty setProperty = (ModelVersionChange.SetProperty) change;
        newProps.put(setProperty.property(), setProperty.value());

      } else if (change instanceof ModelVersionChange.UpdateAliases) {
        ModelVersionChange.UpdateAliases updateAliasesChange =
            (ModelVersionChange.UpdateAliases) change;

        Set<String> addTmpSet = updateAliasesChange.aliasesToAdd();
        Set<String> deleteTmpSet = updateAliasesChange.aliasesToRemove();
        Set<String> aliasToAdd = Sets.difference(addTmpSet, deleteTmpSet).immutableCopy();
        Set<String> aliasToDelete = Sets.difference(deleteTmpSet, addTmpSet).immutableCopy();

        newAliases = doDeleteAlias(newAliases, aliasToDelete);
        newAliases = doSetAlias(newAliases, aliasToAdd);

      } else if (change instanceof ModelVersionChange.UpdateUri) {
        ModelVersionChange.UpdateUri updateUriChange = (ModelVersionChange.UpdateUri) change;
        newUris.replace(updateUriChange.uriName(), updateUriChange.newUri());

      } else if (change instanceof ModelVersionChange.AddUri) {
        ModelVersionChange.AddUri addUriChange = (ModelVersionChange.AddUri) change;
        newUris.putIfAbsent(addUriChange.uriName(), addUriChange.uri());

      } else if (change instanceof ModelVersionChange.RemoveUri) {
        ModelVersionChange.RemoveUri removeUriChange = (ModelVersionChange.RemoveUri) change;
        newUris.remove(removeUriChange.uriName());

      } else {
        throw new IllegalArgumentException("Unsupported model version change: " + change);
      }
    }

    if (newUris.isEmpty()) {
      throw new IllegalArgumentException("Model version URI cannot be empty");
    }

    TestModelVersion updatedModelVersion =
        TestModelVersion.builder()
            .withVersion(newVersion)
            .withComment(newComment)
            .withProperties(newProps)
            .withAuditInfo(updatedAuditInfo)
            .withUris(newUris)
            .withAliases(newAliases)
            .build();

    modelVersions.put(versionPair, updatedModelVersion);

    Arrays.stream(newAliases)
        .map(alias -> Pair.of(ident, alias))
        .forEach(pair -> modelAliasToVersion.put(pair, newVersion));
    return updatedModelVersion;
  }

  private boolean hasCallerContext() {
    return CallerContext.CallerContextHolder.get() != null
        && CallerContext.CallerContextHolder.get().context() != null
        && !CallerContext.CallerContextHolder.get().context().isEmpty();
  }

  private boolean checkSingleFile(String location) {
    try {
      File locationPath = new File(location);
      return locationPath.isFile();
    } catch (Exception e) {
      return false;
    }
  }

  private Map<String, Column> updateColumnPositionsAfterColumnUpdate(
      String updatedColumnName,
      TableChange.ColumnPosition newColumnPosition,
      Map<String, Column> allColumns) {
    TestColumn updatedColumn = (TestColumn) allColumns.get(updatedColumnName);
    int newPosition;
    if (newColumnPosition instanceof TableChange.First) {
      newPosition = 0;
    } else if (newColumnPosition instanceof TableChange.Default) {
      newPosition = allColumns.size() - 1;
    } else if (newColumnPosition instanceof TableChange.After) {
      String afterColumnName = ((TableChange.After) newColumnPosition).getColumn();
      Column afterColumn = allColumns.get(afterColumnName);
      newPosition = ((TestColumn) afterColumn).position() + 1;
    } else {
      throw new IllegalArgumentException("Unsupported column position: " + newColumnPosition);
    }
    updatedColumn.setPosition(newPosition);

    allColumns.forEach(
        (columnName, column) -> {
          if (columnName.equals(updatedColumnName)) {
            return;
          }
          TestColumn testColumn = (TestColumn) column;
          if (testColumn.position() >= newPosition) {
            testColumn.setPosition(testColumn.position() + 1);
          }
        });

    return allColumns;
  }

  private Column[] updateColumns(Column[] columns, TableChange.ColumnChange[] columnChanges) {
    Map<String, Column> columnMap =
        Arrays.stream(columns).collect(Collectors.toMap(Column::name, Function.identity()));

    for (TableChange.ColumnChange columnChange : columnChanges) {
      if (columnChange instanceof TableChange.AddColumn) {
        TableChange.AddColumn addColumn = (TableChange.AddColumn) columnChange;
        TestColumn column =
            TestColumn.builder()
                .withName(String.join(".", addColumn.fieldName()))
                .withPosition(columnMap.size())
                .withComment(addColumn.getComment())
                .withType(addColumn.getDataType())
                .withNullable(addColumn.isNullable())
                .withAutoIncrement(addColumn.isAutoIncrement())
                .withDefaultValue(addColumn.getDefaultValue())
                .build();
        columnMap.put(column.name(), column);
        updateColumnPositionsAfterColumnUpdate(column.name(), addColumn.getPosition(), columnMap);

      } else if (columnChange instanceof TableChange.DeleteColumn) {
        TestColumn removedColumn =
            (TestColumn) columnMap.remove(String.join(".", columnChange.fieldName()));
        columnMap.forEach(
            (columnName, column) -> {
              TestColumn testColumn = (TestColumn) column;
              if (testColumn.position() > removedColumn.position()) {
                testColumn.setPosition(testColumn.position() - 1);
              }
            });

      } else if (columnChange instanceof TableChange.RenameColumn) {
        String oldName = String.join(".", columnChange.fieldName());
        String newName = ((TableChange.RenameColumn) columnChange).getNewName();
        Column column = columnMap.remove(oldName);
        TestColumn newColumn =
            TestColumn.builder()
                .withName(newName)
                .withPosition(((TestColumn) column).position())
                .withComment(column.comment())
                .withType(column.dataType())
                .withNullable(column.nullable())
                .withAutoIncrement(column.autoIncrement())
                .withDefaultValue(column.defaultValue())
                .build();
        columnMap.put(newName, newColumn);

      } else if (columnChange instanceof TableChange.UpdateColumnDefaultValue) {
        String columnName = String.join(".", columnChange.fieldName());
        TableChange.UpdateColumnDefaultValue updateColumnDefaultValue =
            (TableChange.UpdateColumnDefaultValue) columnChange;
        Column oldColumn = columnMap.get(columnName);
        TestColumn newColumn =
            TestColumn.builder()
                .withName(columnName)
                .withPosition(((TestColumn) oldColumn).position())
                .withComment(oldColumn.comment())
                .withType(oldColumn.dataType())
                .withNullable(oldColumn.nullable())
                .withAutoIncrement(oldColumn.autoIncrement())
                .withDefaultValue(updateColumnDefaultValue.getNewDefaultValue())
                .build();
        columnMap.put(columnName, newColumn);

      } else if (columnChange instanceof TableChange.UpdateColumnType) {
        String columnName = String.join(".", columnChange.fieldName());
        TableChange.UpdateColumnType updateColumnType = (TableChange.UpdateColumnType) columnChange;
        Column oldColumn = columnMap.get(columnName);
        TestColumn newColumn =
            TestColumn.builder()
                .withName(columnName)
                .withPosition(((TestColumn) oldColumn).position())
                .withComment(oldColumn.comment())
                .withType(updateColumnType.getNewDataType())
                .withNullable(oldColumn.nullable())
                .withAutoIncrement(oldColumn.autoIncrement())
                .withDefaultValue(oldColumn.defaultValue())
                .build();
        columnMap.put(columnName, newColumn);

      } else if (columnChange instanceof TableChange.UpdateColumnComment) {
        String columnName = String.join(".", columnChange.fieldName());
        TableChange.UpdateColumnComment updateColumnComment =
            (TableChange.UpdateColumnComment) columnChange;
        Column oldColumn = columnMap.get(columnName);
        TestColumn newColumn =
            TestColumn.builder()
                .withName(columnName)
                .withPosition(((TestColumn) oldColumn).position())
                .withComment(updateColumnComment.getNewComment())
                .withType(oldColumn.dataType())
                .withNullable(oldColumn.nullable())
                .withAutoIncrement(oldColumn.autoIncrement())
                .withDefaultValue(oldColumn.defaultValue())
                .build();
        columnMap.put(columnName, newColumn);

      } else if (columnChange instanceof TableChange.UpdateColumnNullability) {
        String columnName = String.join(".", columnChange.fieldName());
        TableChange.UpdateColumnNullability updateColumnNullable =
            (TableChange.UpdateColumnNullability) columnChange;
        Column oldColumn = columnMap.get(columnName);
        TestColumn newColumn =
            TestColumn.builder()
                .withName(columnName)
                .withPosition(((TestColumn) oldColumn).position())
                .withComment(oldColumn.comment())
                .withType(oldColumn.dataType())
                .withNullable(updateColumnNullable.nullable())
                .withAutoIncrement(oldColumn.autoIncrement())
                .withDefaultValue(oldColumn.defaultValue())
                .build();
        columnMap.put(columnName, newColumn);

      } else if (columnChange instanceof TableChange.UpdateColumnAutoIncrement) {
        String columnName = String.join(".", columnChange.fieldName());
        TableChange.UpdateColumnAutoIncrement updateColumnAutoIncrement =
            (TableChange.UpdateColumnAutoIncrement) columnChange;
        Column oldColumn = columnMap.get(columnName);
        TestColumn newColumn =
            TestColumn.builder()
                .withName(columnName)
                .withPosition(((TestColumn) oldColumn).position())
                .withComment(oldColumn.comment())
                .withType(oldColumn.dataType())
                .withNullable(oldColumn.nullable())
                .withAutoIncrement(updateColumnAutoIncrement.isAutoIncrement())
                .withDefaultValue(oldColumn.defaultValue())
                .build();
        columnMap.put(columnName, newColumn);

      } else if (columnChange instanceof TableChange.UpdateColumnPosition) {
        String columnName = String.join(".", columnChange.fieldName());
        TableChange.UpdateColumnPosition updateColumnPosition =
            (TableChange.UpdateColumnPosition) columnChange;
        columnMap =
            updateColumnPositionsAfterColumnUpdate(
                columnName, updateColumnPosition.getPosition(), columnMap);

      } else {
        throw new IllegalArgumentException("Unsupported column change: " + columnChange);
      }
    }

    return columnMap.values().stream()
        .map(TestColumn.class::cast)
        .sorted(Comparator.comparingInt(TestColumn::position))
        .toArray(TestColumn[]::new);
  }

  private String[] doDeleteAlias(String[] entityAliases, Set<String> aliasToDelete) {
    List<String> aliasList = new ArrayList<>(Arrays.asList(entityAliases));
    aliasList.removeAll(aliasToDelete);

    return aliasList.toArray(new String[0]);
  }

  private String[] doSetAlias(String[] entityAliases, Set<String> aliasToAdd) {
    List<String> aliasList = new ArrayList<>(Arrays.asList(entityAliases));
    Set<String> aliasSet = new HashSet<>(aliasList);

    for (String alias : aliasToAdd) {
      if (aliasSet.add(alias)) {
        aliasList.add(alias);
      }
    }

    return aliasList.toArray(new String[0]);
  }
}
