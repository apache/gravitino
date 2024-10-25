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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.TestColumn;
import org.apache.gravitino.TestFileset;
import org.apache.gravitino.TestSchema;
import org.apache.gravitino.TestTable;
import org.apache.gravitino.TestTopic;
import org.apache.gravitino.audit.CallerContext;
import org.apache.gravitino.audit.FilesetAuditConstants;
import org.apache.gravitino.audit.FilesetDataOperation;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
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
    implements CatalogOperations, TableCatalog, FilesetCatalog, TopicCatalog, SupportsSchemas {
  private static final Logger LOG = LoggerFactory.getLogger(TestCatalogOperations.class);

  private final Map<NameIdentifier, TestTable> tables;

  private final Map<NameIdentifier, TestSchema> schemas;

  private final Map<NameIdentifier, TestFileset> filesets;

  private final Map<NameIdentifier, TestTopic> topics;

  public static final String FAIL_CREATE = "fail-create";

  public static final String FAIL_TEST = "need-fail";

  private static final String SLASH = "/";

  public TestCatalogOperations(Map<String, String> config) {
    tables = Maps.newHashMap();
    schemas = Maps.newHashMap();
    filesets = Maps.newHashMap();
    topics = Maps.newHashMap();
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
  public Fileset createFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      String storageLocation,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    TestFileset fileset =
        TestFileset.builder()
            .withName(ident.name())
            .withComment(comment)
            .withProperties(properties)
            .withAuditInfo(auditInfo)
            .withType(type)
            .withStorageLocation(storageLocation)
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
            .withStorageLocation(fileset.storageLocation())
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
  public String getFileLocation(NameIdentifier ident, String subPath) {
    Preconditions.checkArgument(subPath != null, "subPath must not be null");
    String processedSubPath;
    if (!subPath.trim().isEmpty() && !subPath.trim().startsWith(SLASH)) {
      processedSubPath = SLASH + subPath.trim();
    } else {
      processedSubPath = subPath.trim();
    }

    Fileset fileset = loadFileset(ident);

    boolean isSingleFile = checkSingleFile(fileset);
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

  private boolean hasCallerContext() {
    return CallerContext.CallerContextHolder.get() != null
        && CallerContext.CallerContextHolder.get().context() != null
        && !CallerContext.CallerContextHolder.get().context().isEmpty();
  }

  private boolean checkSingleFile(Fileset fileset) {
    try {
      File locationPath = new File(fileset.storageLocation());
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
}
