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

package org.apache.gravitino.audit;

import com.google.common.collect.ImmutableMap;
import java.time.LocalDate;
import java.util.HashMap;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.listener.api.event.AlterCatalogEvent;
import org.apache.gravitino.listener.api.event.AlterCatalogFailureEvent;
import org.apache.gravitino.listener.api.event.AlterFilesetEvent;
import org.apache.gravitino.listener.api.event.AlterFilesetFailureEvent;
import org.apache.gravitino.listener.api.event.AlterMetalakeEvent;
import org.apache.gravitino.listener.api.event.AlterMetalakeFailureEvent;
import org.apache.gravitino.listener.api.event.AlterSchemaEvent;
import org.apache.gravitino.listener.api.event.AlterSchemaFailureEvent;
import org.apache.gravitino.listener.api.event.AlterTableEvent;
import org.apache.gravitino.listener.api.event.AlterTableFailureEvent;
import org.apache.gravitino.listener.api.event.AlterTopicEvent;
import org.apache.gravitino.listener.api.event.AlterTopicFailureEvent;
import org.apache.gravitino.listener.api.event.CreateCatalogEvent;
import org.apache.gravitino.listener.api.event.CreateCatalogFailureEvent;
import org.apache.gravitino.listener.api.event.CreateFilesetEvent;
import org.apache.gravitino.listener.api.event.CreateFilesetFailureEvent;
import org.apache.gravitino.listener.api.event.CreateMetalakeEvent;
import org.apache.gravitino.listener.api.event.CreateMetalakeFailureEvent;
import org.apache.gravitino.listener.api.event.CreateSchemaEvent;
import org.apache.gravitino.listener.api.event.CreateSchemaFailureEvent;
import org.apache.gravitino.listener.api.event.CreateTableEvent;
import org.apache.gravitino.listener.api.event.CreateTableFailureEvent;
import org.apache.gravitino.listener.api.event.CreateTopicEvent;
import org.apache.gravitino.listener.api.event.CreateTopicFailureEvent;
import org.apache.gravitino.listener.api.event.DropCatalogEvent;
import org.apache.gravitino.listener.api.event.DropCatalogFailureEvent;
import org.apache.gravitino.listener.api.event.DropFilesetEvent;
import org.apache.gravitino.listener.api.event.DropFilesetFailureEvent;
import org.apache.gravitino.listener.api.event.DropMetalakeEvent;
import org.apache.gravitino.listener.api.event.DropMetalakeFailureEvent;
import org.apache.gravitino.listener.api.event.DropSchemaEvent;
import org.apache.gravitino.listener.api.event.DropSchemaFailureEvent;
import org.apache.gravitino.listener.api.event.DropTableEvent;
import org.apache.gravitino.listener.api.event.DropTableFailureEvent;
import org.apache.gravitino.listener.api.event.DropTopicEvent;
import org.apache.gravitino.listener.api.event.DropTopicFailureEvent;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.GetFileLocationEvent;
import org.apache.gravitino.listener.api.event.GetFileLocationFailureEvent;
import org.apache.gravitino.listener.api.event.GetPartitionEvent;
import org.apache.gravitino.listener.api.event.GetPartitionFailureEvent;
import org.apache.gravitino.listener.api.event.ListCatalogEvent;
import org.apache.gravitino.listener.api.event.ListCatalogFailureEvent;
import org.apache.gravitino.listener.api.event.ListFilesetEvent;
import org.apache.gravitino.listener.api.event.ListFilesetFailureEvent;
import org.apache.gravitino.listener.api.event.ListPartitionEvent;
import org.apache.gravitino.listener.api.event.ListPartitionFailureEvent;
import org.apache.gravitino.listener.api.event.ListSchemaEvent;
import org.apache.gravitino.listener.api.event.ListSchemaFailureEvent;
import org.apache.gravitino.listener.api.event.ListTableEvent;
import org.apache.gravitino.listener.api.event.ListTableFailureEvent;
import org.apache.gravitino.listener.api.event.ListTopicEvent;
import org.apache.gravitino.listener.api.event.ListTopicFailureEvent;
import org.apache.gravitino.listener.api.event.LoadCatalogEvent;
import org.apache.gravitino.listener.api.event.LoadCatalogFailureEvent;
import org.apache.gravitino.listener.api.event.LoadFilesetEvent;
import org.apache.gravitino.listener.api.event.LoadFilesetFailureEvent;
import org.apache.gravitino.listener.api.event.LoadMetalakeEvent;
import org.apache.gravitino.listener.api.event.LoadMetalakeFailureEvent;
import org.apache.gravitino.listener.api.event.LoadSchemaEvent;
import org.apache.gravitino.listener.api.event.LoadSchemaFailureEvent;
import org.apache.gravitino.listener.api.event.LoadTableEvent;
import org.apache.gravitino.listener.api.event.LoadTableFailureEvent;
import org.apache.gravitino.listener.api.event.LoadTopicEvent;
import org.apache.gravitino.listener.api.event.LoadTopicFailureEvent;
import org.apache.gravitino.listener.api.event.PartitionExistsEvent;
import org.apache.gravitino.listener.api.event.PurgePartitionEvent;
import org.apache.gravitino.listener.api.event.PurgePartitionFailureEvent;
import org.apache.gravitino.listener.api.event.PurgeTableEvent;
import org.apache.gravitino.listener.api.info.CatalogInfo;
import org.apache.gravitino.listener.api.info.FilesetInfo;
import org.apache.gravitino.listener.api.info.MetalakeInfo;
import org.apache.gravitino.listener.api.info.SchemaInfo;
import org.apache.gravitino.listener.api.info.TableInfo;
import org.apache.gravitino.listener.api.info.TopicInfo;
import org.apache.gravitino.listener.api.info.partitions.IdentityPartitionInfo;
import org.apache.gravitino.listener.api.info.partitions.PartitionInfo;
import org.apache.gravitino.messaging.TopicChange;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOperation {

  private NameIdentifier metalakeIdentifier;

  private MetalakeInfo metalakeInfo;

  private NameIdentifier catalogIdentifier;

  private CatalogInfo catalogInfo;

  private NameIdentifier schemaIdentifier;

  private SchemaInfo schemaInfo;

  private NameIdentifier tableIdentifier;

  private TableInfo tableInfo;

  private NameIdentifier filesetIdentifier;

  private FilesetInfo filesetInfo;

  private NameIdentifier topicIdentifier;

  private TopicInfo topicInfo;

  private NameIdentifier partitionIdentifier;

  private PartitionInfo partitionInfo;

  private static final String USER = "user";

  private static final String PARTITION_NAME = "dt=2008-08-08/country=us";

  @BeforeAll
  public void init() {
    this.metalakeIdentifier = mockMetalakeIdentifier();
    this.metalakeInfo = mockMetalakeInfo();

    this.catalogIdentifier = mockCatalogIndentifier();
    this.catalogInfo = mockCatalogInfo();

    this.schemaIdentifier = mockSchemaIdentifier();
    this.schemaInfo = mockSchemaInfo();

    this.tableIdentifier = mockTableIdentifier();
    this.tableInfo = mockTableInfo();

    this.topicIdentifier = mockTopicIdentifier();
    this.topicInfo = mockTopicInfo();

    this.filesetIdentifier = mockFilesetIdentifier();
    this.filesetInfo = mockFilesetInfo();

    this.partitionIdentifier = mockPartitionIdentifier();
    this.partitionInfo = mockPartitionInfo();
  }

  @Test
  public void testCreateOperation() {
    Event createMetalakeEvent = new CreateMetalakeEvent(USER, catalogIdentifier, metalakeInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createMetalakeEvent), AuditLog.Operation.CREATE_METALAKE);
    Event createMetalakeFaileEvent =
        new CreateMetalakeFailureEvent(USER, metalakeIdentifier, new Exception(), metalakeInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createMetalakeFaileEvent), AuditLog.Operation.CREATE_METALAKE);

    Event createCatalogEvent = new CreateCatalogEvent(USER, catalogIdentifier, catalogInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createCatalogEvent), AuditLog.Operation.CREATE_CATALOG);
    Event CreateCatalogFailureEvent =
        new CreateCatalogFailureEvent(USER, catalogIdentifier, new Exception(), catalogInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(CreateCatalogFailureEvent), AuditLog.Operation.CREATE_CATALOG);

    Event createSchemaEvent = new CreateSchemaEvent(USER, schemaIdentifier, schemaInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createSchemaEvent), AuditLog.Operation.CREATE_SCHEMA);
    Event createSchemaFailureEvent =
        new CreateSchemaFailureEvent(USER, schemaIdentifier, new Exception(), schemaInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createSchemaFailureEvent), AuditLog.Operation.CREATE_SCHEMA);

    Event createTableEvent = new CreateTableEvent(USER, tableIdentifier, tableInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createTableEvent), AuditLog.Operation.CREATE_TABLE);
    Event createTableFailureEvent =
        new CreateTableFailureEvent(USER, tableIdentifier, new Exception(), tableInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createTableFailureEvent), AuditLog.Operation.CREATE_TABLE);

    Event createFilesetEvent = new CreateFilesetEvent(USER, filesetIdentifier, filesetInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createFilesetEvent), AuditLog.Operation.CREATE_FILESET);
    Event createFilesetFailureEvent =
        new CreateFilesetFailureEvent(USER, filesetIdentifier, new Exception(), filesetInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createFilesetFailureEvent), AuditLog.Operation.CREATE_FILESET);

    Event createTopicEvent = new CreateTopicEvent(USER, topicIdentifier, topicInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createTopicEvent), AuditLog.Operation.CREATE_TOPIC);
    Event createTopicFailureEvent =
        new CreateTopicFailureEvent(USER, topicIdentifier, new Exception(), topicInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createTopicFailureEvent), AuditLog.Operation.CREATE_TOPIC);
  }

  @Test
  public void testAlterOperation() {
    Event alterMetalakeEvent =
        new AlterMetalakeEvent(USER, metalakeIdentifier, new MetalakeChange[] {}, metalakeInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterMetalakeEvent), AuditLog.Operation.ALTER_METALAKE);
    Event alterMetalakeFailureEvent =
        new AlterMetalakeFailureEvent(
            USER, metalakeIdentifier, new Exception(), new MetalakeChange[] {});
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterMetalakeFailureEvent), AuditLog.Operation.ALTER_METALAKE);

    Event alterCatalogEvent =
        new AlterCatalogEvent(USER, schemaIdentifier, new CatalogChange[] {}, catalogInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterCatalogEvent), AuditLog.Operation.ALTER_CATALOG);
    Event alterCatalogFailureEvent =
        new AlterCatalogFailureEvent(
            USER, catalogIdentifier, new Exception(), new CatalogChange[] {});
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterCatalogFailureEvent), AuditLog.Operation.ALTER_CATALOG);

    Event alterSchemaEvent =
        new AlterSchemaEvent(USER, schemaIdentifier, new SchemaChange[] {}, schemaInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterSchemaEvent), AuditLog.Operation.ALTER_SCHEMA);
    Event alterSchemaFailureEvent =
        new AlterSchemaFailureEvent(USER, schemaIdentifier, new Exception(), new SchemaChange[] {});
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterSchemaFailureEvent), AuditLog.Operation.ALTER_SCHEMA);

    Event alterTableEvent =
        new AlterTableEvent(USER, tableIdentifier, new TableChange[] {}, tableInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterTableEvent), AuditLog.Operation.ALTER_TABLE);
    Event alterTableFailureEvent =
        new AlterTableFailureEvent(USER, tableIdentifier, new Exception(), new TableChange[] {});
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterTableFailureEvent), AuditLog.Operation.ALTER_TABLE);

    Event alterFilesetEvent =
        new AlterFilesetEvent(USER, filesetIdentifier, new FilesetChange[] {}, filesetInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterFilesetEvent), AuditLog.Operation.ALTER_FILESET);
    Event alterFilesetFailureEvent =
        new AlterFilesetFailureEvent(
            USER, filesetIdentifier, new Exception(), new FilesetChange[] {});
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterFilesetFailureEvent), AuditLog.Operation.ALTER_FILESET);

    Event alterTopicEvent =
        new AlterTopicEvent(USER, topicIdentifier, new TopicChange[] {}, topicInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterTopicEvent), AuditLog.Operation.ALTER_TOPIC);
    Event alterTopicFailureEvent =
        new AlterTopicFailureEvent(USER, topicIdentifier, new Exception(), new TopicChange[] {});
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterTopicFailureEvent), AuditLog.Operation.ALTER_TOPIC);
  }

  @Test
  public void testDropOperation() {
    Event dropMetalakeEvent = new DropMetalakeEvent(USER, metalakeIdentifier, true);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropMetalakeEvent), AuditLog.Operation.DROP_METALAKE);
    Event dropMetalakeFailureEvent = new DropMetalakeFailureEvent(USER, metalakeIdentifier, null);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropMetalakeFailureEvent), AuditLog.Operation.DROP_METALAKE);

    Event dropCatalogEvent = new DropCatalogEvent(USER, catalogIdentifier, true);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropCatalogEvent), AuditLog.Operation.DROP_CATALOG);
    Event dropCatalogFailureEvent =
        new DropCatalogFailureEvent(USER, catalogIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropCatalogFailureEvent), AuditLog.Operation.DROP_CATALOG);

    Event dropSchemaEvent = new DropSchemaEvent(USER, schemaIdentifier, true, true);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropSchemaEvent), AuditLog.Operation.DROP_SCHEMA);
    Event dropSchemaFailureEvent =
        new DropSchemaFailureEvent(USER, schemaIdentifier, new Exception(), true);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropSchemaFailureEvent), AuditLog.Operation.DROP_SCHEMA);

    Event dropTableEvent = new DropTableEvent(USER, tableIdentifier, true);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropTableEvent), AuditLog.Operation.DROP_TABLE);
    Event dropTableFailureEvent = new DropTableFailureEvent(USER, tableIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropTableFailureEvent), AuditLog.Operation.DROP_TABLE);

    Event dropFilesetEvent = new DropFilesetEvent(USER, filesetIdentifier, true);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropFilesetEvent), AuditLog.Operation.DROP_FILESET);
    Event dropFilesetFailureEvent =
        new DropFilesetFailureEvent(USER, filesetIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropFilesetFailureEvent), AuditLog.Operation.DROP_FILESET);

    Event dropTopicEvent = new DropTopicEvent(USER, topicIdentifier, true);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropTopicEvent), AuditLog.Operation.DROP_TOPIC);
    Event dropTopicFailureEvent = new DropTopicFailureEvent(USER, topicIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropTopicFailureEvent), AuditLog.Operation.DROP_TOPIC);
  }

  @Test
  public void testPurgeOperation() {
    Event purgeMetalakeEvent =
        new PurgePartitionEvent(USER, metalakeIdentifier, true, PARTITION_NAME);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(purgeMetalakeEvent), AuditLog.Operation.PURGE_PARTITION);
    Event purgeMetalakeFailureEvent =
        new PurgePartitionFailureEvent(USER, partitionIdentifier, new Exception(), PARTITION_NAME);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(purgeMetalakeFailureEvent),
        AuditLog.Operation.PURGE_PARTITION);

    Event purgeTableEvent = new PurgeTableEvent(USER, tableIdentifier, true);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(purgeTableEvent), AuditLog.Operation.PURGE_TABLE);
    Event purgePartitionFailureEvent =
        new PurgePartitionFailureEvent(USER, partitionIdentifier, new Exception(), PARTITION_NAME);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(purgePartitionFailureEvent),
        AuditLog.Operation.PURGE_PARTITION);
  }

  @Test
  public void testGetOperation() {
    Event getFileLocationEvent =
        new GetFileLocationEvent(USER, filesetIdentifier, "location", "subPath", new HashMap<>());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(getFileLocationEvent), AuditLog.Operation.GET_FILE_LOCATION);
    Event getFilesetFailureEvent =
        new GetFileLocationFailureEvent(USER, filesetIdentifier, "subPath", new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(getFilesetFailureEvent), AuditLog.Operation.GET_FILE_LOCATION);

    Event getPartitionEvent = new GetPartitionEvent(USER, partitionIdentifier, partitionInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(getPartitionEvent), AuditLog.Operation.GET_PARTITION);
    Event getPartitionFailureEvent =
        new GetPartitionFailureEvent(USER, partitionIdentifier, new Exception(), PARTITION_NAME);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(getPartitionFailureEvent), AuditLog.Operation.GET_PARTITION);
  }

  @Test
  public void testListOperation() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    Event listCatalogEvent = new ListCatalogEvent(USER, namespace);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listCatalogEvent), AuditLog.Operation.LIST_CATALOG);
    Event listCatalogFailureEvent = new ListCatalogFailureEvent(USER, new Exception(), namespace);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listCatalogFailureEvent), AuditLog.Operation.LIST_CATALOG);

    Event listSchemaEvent = new ListSchemaEvent(USER, namespace);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listSchemaEvent), AuditLog.Operation.LIST_SCHEMA);
    Event listSchemaFailureEvent = new ListSchemaFailureEvent(USER, namespace, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listSchemaFailureEvent), AuditLog.Operation.LIST_SCHEMA);

    Event listTableEvent = new ListTableEvent(USER, namespace);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listTableEvent), AuditLog.Operation.LIST_TABLE);
    Event listTableFailureEvent = new ListTableFailureEvent(USER, namespace, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listTableFailureEvent), AuditLog.Operation.LIST_TABLE);

    Event listTopicEvent = new ListTopicEvent(USER, namespace);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listTopicEvent), AuditLog.Operation.LIST_TOPIC);
    Event listTopicFailureEvent = new ListTopicFailureEvent(USER, namespace, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listTopicFailureEvent), AuditLog.Operation.LIST_TOPIC);

    Event listFilesetEvent = new ListFilesetEvent(USER, namespace);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listFilesetEvent), AuditLog.Operation.LIST_FILESET);
    Event listFilesetFailureEvent = new ListFilesetFailureEvent(USER, namespace, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listFilesetFailureEvent), AuditLog.Operation.LIST_FILESET);

    Event listPartitionEvent = new ListPartitionEvent(USER, partitionIdentifier);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listPartitionEvent), AuditLog.Operation.LIST_PARTITION);
    Event listPartitionFailureEvent =
        new ListPartitionFailureEvent(USER, partitionIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listPartitionFailureEvent), AuditLog.Operation.LIST_PARTITION);
  }

  @Test
  public void testLoadOperation() {
    Event loadMetalakeEvent = new LoadMetalakeEvent(USER, metalakeIdentifier, metalakeInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadMetalakeEvent), AuditLog.Operation.LOAD_METALAKE);
    Event loadMetalakeFailureEvent =
        new LoadMetalakeFailureEvent(USER, metalakeIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadMetalakeFailureEvent), AuditLog.Operation.LOAD_METALAKE);

    Event loadCatalogEvent = new LoadCatalogEvent(USER, catalogIdentifier, catalogInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadCatalogEvent), AuditLog.Operation.LOAD_CATALOG);
    Event loadCatalogFailureEvent =
        new LoadCatalogFailureEvent(USER, catalogIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadCatalogFailureEvent), AuditLog.Operation.LOAD_CATALOG);

    Event loadSchemaEvent = new LoadSchemaEvent(USER, schemaIdentifier, schemaInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadSchemaEvent), AuditLog.Operation.LOAD_SCHEMA);
    Event loadSchemaFailureEvent =
        new LoadSchemaFailureEvent(USER, schemaIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadSchemaFailureEvent), AuditLog.Operation.LOAD_SCHEMA);

    Event loadTableEvent = new LoadTableEvent(USER, tableIdentifier, tableInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadTableEvent), AuditLog.Operation.LOAD_TABLE);
    Event loadTableFailureEvent = new LoadTableFailureEvent(USER, tableIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadTableFailureEvent), AuditLog.Operation.LOAD_TABLE);

    Event loadFilesetEvent = new LoadFilesetEvent(USER, filesetIdentifier, filesetInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadFilesetEvent), AuditLog.Operation.LOAD_FILESET);
    Event loadFilesetFailureEvent =
        new LoadFilesetFailureEvent(USER, filesetIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadFilesetFailureEvent), AuditLog.Operation.LOAD_FILESET);

    Event loadTopicEvent = new LoadTopicEvent(USER, topicIdentifier, topicInfo);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadTopicEvent), AuditLog.Operation.LOAD_TOPIC);
    Event loadTopicFailureEvent = new LoadTopicFailureEvent(USER, topicIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadTopicFailureEvent), AuditLog.Operation.LOAD_TOPIC);
  }

  @Test
  public void testExistsOperation() {
    Event partitionExistsEvent =
        new PartitionExistsEvent(USER, partitionIdentifier, true, partitionIdentifier.name());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(partitionExistsEvent), AuditLog.Operation.PARTITION_EXIST);

    Event partitionExistsFailureEvent =
        new PartitionExistsEvent(USER, partitionIdentifier, true, partitionIdentifier.name());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(partitionExistsFailureEvent),
        AuditLog.Operation.PARTITION_EXIST);
  }

  private NameIdentifier mockMetalakeIdentifier() {
    return NameIdentifier.of("metalake");
  }

  private MetalakeInfo mockMetalakeInfo() {
    return new MetalakeInfo("metalake", "comment", ImmutableMap.of("a", "b"), null);
  }

  private NameIdentifier mockCatalogIndentifier() {
    return NameIdentifier.of("metalake", "catalog");
  }

  private CatalogInfo mockCatalogInfo() {
    return new CatalogInfo(
        "catalog", Catalog.Type.RELATIONAL, "hive", "comment", ImmutableMap.of("a", "b"), null);
  }

  private NameIdentifier mockSchemaIdentifier() {
    return NameIdentifier.of("metalake", "catalog", "schema");
  }

  private SchemaInfo mockSchemaInfo() {
    return new SchemaInfo("schema", "comment", ImmutableMap.of("a", "b"), null);
  }

  private NameIdentifier mockTableIdentifier() {
    return NameIdentifier.of("metalake", "catalog", "table");
  }

  private TableInfo mockTableInfo() {
    return new TableInfo(
        "table",
        new Column[] {Column.of("a", Types.IntegerType.get())},
        "comment",
        ImmutableMap.of("a", "b"),
        new Transform[] {Transforms.identity("a")},
        Distributions.of(Strategy.HASH, 10, NamedReference.field("a")),
        new SortOrder[] {SortOrders.ascending(NamedReference.field("a"))},
        new Index[] {Indexes.primary("p", new String[][] {{"a"}, {"b"}})},
        null);
  }

  private NameIdentifier mockFilesetIdentifier() {
    return NameIdentifier.of("metalake", "catalog", "fileset");
  }

  private FilesetInfo mockFilesetInfo() {
    return new FilesetInfo(
        "fileset", "comment", Fileset.Type.MANAGED, "location", ImmutableMap.of("a", "b"), null);
  }

  private NameIdentifier mockTopicIdentifier() {
    return NameIdentifier.of("metalake", "catalog", "topic");
  }

  private TopicInfo mockTopicInfo() {
    return new TopicInfo("topic", "comment", ImmutableMap.of("a", "b"), null);
  }

  private NameIdentifier mockPartitionIdentifier() {
    return NameIdentifier.of("metalake", "catalog", "schema", "table", PARTITION_NAME);
  }

  private PartitionInfo mockPartitionInfo() {
    return new IdentityPartitionInfo(
        PARTITION_NAME,
        new String[][] {{"dt"}, {"country"}},
        new Literal[] {
          Literals.dateLiteral(LocalDate.parse("2008-08-08")), Literals.stringLiteral("us")
        },
        ImmutableMap.of("location", "/user/hive/warehouse/tpch_flat_orc_2.db/orders"));
  }
}
