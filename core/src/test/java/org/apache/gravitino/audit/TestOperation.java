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
import org.apache.gravitino.listener.api.event.DisableCatalogEvent;
import org.apache.gravitino.listener.api.event.DisableCatalogFailureEvent;
import org.apache.gravitino.listener.api.event.DisableMetalakeEvent;
import org.apache.gravitino.listener.api.event.DisableMetalakeFailureEvent;
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
import org.apache.gravitino.listener.api.event.EnableCatalogEvent;
import org.apache.gravitino.listener.api.event.EnableCatalogFailureEvent;
import org.apache.gravitino.listener.api.event.EnableMetalakeEvent;
import org.apache.gravitino.listener.api.event.EnableMetalakeFailureEvent;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.EventSource;
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
import org.apache.gravitino.listener.api.event.server.AuthorizationDenialFailureEvent;
import org.apache.gravitino.listener.api.event.server.HttpRequestFailureEvent;
import org.apache.gravitino.listener.api.event.view.AlterViewEvent;
import org.apache.gravitino.listener.api.event.view.AlterViewFailureEvent;
import org.apache.gravitino.listener.api.event.view.CreateViewEvent;
import org.apache.gravitino.listener.api.event.view.CreateViewFailureEvent;
import org.apache.gravitino.listener.api.event.view.DropViewEvent;
import org.apache.gravitino.listener.api.event.view.DropViewFailureEvent;
import org.apache.gravitino.listener.api.event.view.ListViewEvent;
import org.apache.gravitino.listener.api.event.view.ListViewFailureEvent;
import org.apache.gravitino.listener.api.event.view.LoadViewEvent;
import org.apache.gravitino.listener.api.event.view.LoadViewFailureEvent;
import org.apache.gravitino.listener.api.info.CatalogInfo;
import org.apache.gravitino.listener.api.info.FilesetInfo;
import org.apache.gravitino.listener.api.info.MetalakeInfo;
import org.apache.gravitino.listener.api.info.SchemaInfo;
import org.apache.gravitino.listener.api.info.TableInfo;
import org.apache.gravitino.listener.api.info.TopicInfo;
import org.apache.gravitino.listener.api.info.ViewInfo;
import org.apache.gravitino.listener.api.info.partitions.IdentityPartitionInfo;
import org.apache.gravitino.listener.api.info.partitions.PartitionInfo;
import org.apache.gravitino.messaging.TopicChange;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.ViewChange;
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

  private NameIdentifier viewIdentifier;

  private ViewInfo viewInfo;

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

    this.viewIdentifier = mockViewIdentifier();
    this.viewInfo = mockViewInfo();

    this.filesetIdentifier = mockFilesetIdentifier();
    this.filesetInfo = mockFilesetInfo();

    this.partitionIdentifier = mockPartitionIdentifier();
    this.partitionInfo = mockPartitionInfo();
  }

  @Test
  public void testCreateOperation() {
    Event createMetalakeEvent = new CreateMetalakeEvent(USER, catalogIdentifier, metalakeInfo);
    Assertions.assertEquals(
        AuditLog.Operation.CREATE_METALAKE, AuditLog.Operation.fromEvent(createMetalakeEvent));
    Event createMetalakeFaileEvent =
        new CreateMetalakeFailureEvent(USER, metalakeIdentifier, new Exception(), metalakeInfo);
    Assertions.assertEquals(
        AuditLog.Operation.CREATE_METALAKE, AuditLog.Operation.fromEvent(createMetalakeFaileEvent));

    Event createCatalogEvent = new CreateCatalogEvent(USER, catalogIdentifier, catalogInfo);
    Assertions.assertEquals(
        AuditLog.Operation.CREATE_CATALOG, AuditLog.Operation.fromEvent(createCatalogEvent));
    Event CreateCatalogFailureEvent =
        new CreateCatalogFailureEvent(USER, catalogIdentifier, new Exception(), catalogInfo);
    Assertions.assertEquals(
        AuditLog.Operation.CREATE_CATALOG, AuditLog.Operation.fromEvent(CreateCatalogFailureEvent));

    Event createSchemaEvent = new CreateSchemaEvent(USER, schemaIdentifier, schemaInfo);
    Assertions.assertEquals(
        AuditLog.Operation.CREATE_SCHEMA, AuditLog.Operation.fromEvent(createSchemaEvent));
    Event createSchemaFailureEvent =
        new CreateSchemaFailureEvent(USER, schemaIdentifier, new Exception(), schemaInfo);
    Assertions.assertEquals(
        AuditLog.Operation.CREATE_SCHEMA, AuditLog.Operation.fromEvent(createSchemaFailureEvent));

    Event createTableEvent = new CreateTableEvent(USER, tableIdentifier, tableInfo);
    Assertions.assertEquals(
        AuditLog.Operation.CREATE_TABLE, AuditLog.Operation.fromEvent(createTableEvent));
    Event createTableFailureEvent =
        new CreateTableFailureEvent(USER, tableIdentifier, new Exception(), tableInfo);
    Assertions.assertEquals(
        AuditLog.Operation.CREATE_TABLE, AuditLog.Operation.fromEvent(createTableFailureEvent));

    Event createFilesetEvent = new CreateFilesetEvent(USER, filesetIdentifier, filesetInfo);
    Assertions.assertEquals(
        AuditLog.Operation.CREATE_FILESET, AuditLog.Operation.fromEvent(createFilesetEvent));
    Event createFilesetFailureEvent =
        new CreateFilesetFailureEvent(USER, filesetIdentifier, new Exception(), filesetInfo);
    Assertions.assertEquals(
        AuditLog.Operation.CREATE_FILESET, AuditLog.Operation.fromEvent(createFilesetFailureEvent));

    Event createTopicEvent = new CreateTopicEvent(USER, topicIdentifier, topicInfo);
    Assertions.assertEquals(
        AuditLog.Operation.CREATE_TOPIC, AuditLog.Operation.fromEvent(createTopicEvent));
    Event createTopicFailureEvent =
        new CreateTopicFailureEvent(USER, topicIdentifier, new Exception(), topicInfo);
    Assertions.assertEquals(
        AuditLog.Operation.CREATE_TOPIC, AuditLog.Operation.fromEvent(createTopicFailureEvent));

    Event createViewEvent = new CreateViewEvent(USER, viewIdentifier, viewInfo);
    Assertions.assertEquals(
        AuditLog.Operation.CREATE_VIEW, AuditLog.Operation.fromEvent(createViewEvent));
    Event createViewFailureEvent =
        new CreateViewFailureEvent(USER, viewIdentifier, new Exception(), viewInfo);
    Assertions.assertEquals(
        AuditLog.Operation.CREATE_VIEW, AuditLog.Operation.fromEvent(createViewFailureEvent));
  }

  @Test
  public void testAlterOperation() {
    Event alterMetalakeEvent =
        new AlterMetalakeEvent(USER, metalakeIdentifier, new MetalakeChange[] {}, metalakeInfo);
    Assertions.assertEquals(
        AuditLog.Operation.ALTER_METALAKE, AuditLog.Operation.fromEvent(alterMetalakeEvent));
    Event alterMetalakeFailureEvent =
        new AlterMetalakeFailureEvent(
            USER, metalakeIdentifier, new Exception(), new MetalakeChange[] {});
    Assertions.assertEquals(
        AuditLog.Operation.ALTER_METALAKE, AuditLog.Operation.fromEvent(alterMetalakeFailureEvent));

    Event alterCatalogEvent =
        new AlterCatalogEvent(USER, schemaIdentifier, new CatalogChange[] {}, catalogInfo);
    Assertions.assertEquals(
        AuditLog.Operation.ALTER_CATALOG, AuditLog.Operation.fromEvent(alterCatalogEvent));
    Event alterCatalogFailureEvent =
        new AlterCatalogFailureEvent(
            USER, catalogIdentifier, new Exception(), new CatalogChange[] {});
    Assertions.assertEquals(
        AuditLog.Operation.ALTER_CATALOG, AuditLog.Operation.fromEvent(alterCatalogFailureEvent));

    Event alterSchemaEvent =
        new AlterSchemaEvent(USER, schemaIdentifier, new SchemaChange[] {}, schemaInfo);
    Assertions.assertEquals(
        AuditLog.Operation.ALTER_SCHEMA, AuditLog.Operation.fromEvent(alterSchemaEvent));
    Event alterSchemaFailureEvent =
        new AlterSchemaFailureEvent(USER, schemaIdentifier, new Exception(), new SchemaChange[] {});
    Assertions.assertEquals(
        AuditLog.Operation.ALTER_SCHEMA, AuditLog.Operation.fromEvent(alterSchemaFailureEvent));

    Event alterTableEvent =
        new AlterTableEvent(USER, tableIdentifier, new TableChange[] {}, tableInfo);
    Assertions.assertEquals(
        AuditLog.Operation.ALTER_TABLE, AuditLog.Operation.fromEvent(alterTableEvent));
    Event alterTableFailureEvent =
        new AlterTableFailureEvent(USER, tableIdentifier, new Exception(), new TableChange[] {});
    Assertions.assertEquals(
        AuditLog.Operation.ALTER_TABLE, AuditLog.Operation.fromEvent(alterTableFailureEvent));

    Event alterFilesetEvent =
        new AlterFilesetEvent(USER, filesetIdentifier, new FilesetChange[] {}, filesetInfo);
    Assertions.assertEquals(
        AuditLog.Operation.ALTER_FILESET, AuditLog.Operation.fromEvent(alterFilesetEvent));
    Event alterFilesetFailureEvent =
        new AlterFilesetFailureEvent(
            USER, filesetIdentifier, new Exception(), new FilesetChange[] {});
    Assertions.assertEquals(
        AuditLog.Operation.ALTER_FILESET, AuditLog.Operation.fromEvent(alterFilesetFailureEvent));

    Event alterTopicEvent =
        new AlterTopicEvent(USER, topicIdentifier, new TopicChange[] {}, topicInfo);
    Assertions.assertEquals(
        AuditLog.Operation.ALTER_TOPIC, AuditLog.Operation.fromEvent(alterTopicEvent));
    Event alterTopicFailureEvent =
        new AlterTopicFailureEvent(USER, topicIdentifier, new Exception(), new TopicChange[] {});
    Assertions.assertEquals(
        AuditLog.Operation.ALTER_TOPIC, AuditLog.Operation.fromEvent(alterTopicFailureEvent));

    Event alterViewEvent = new AlterViewEvent(USER, viewIdentifier, new ViewChange[] {}, viewInfo);
    Assertions.assertEquals(
        AuditLog.Operation.ALTER_VIEW, AuditLog.Operation.fromEvent(alterViewEvent));
    Event alterViewFailureEvent =
        new AlterViewFailureEvent(USER, viewIdentifier, new Exception(), new ViewChange[] {});
    Assertions.assertEquals(
        AuditLog.Operation.ALTER_VIEW, AuditLog.Operation.fromEvent(alterViewFailureEvent));
  }

  @Test
  public void testDropOperation() {
    Event dropMetalakeEvent = new DropMetalakeEvent(USER, metalakeIdentifier, true);
    Assertions.assertEquals(
        AuditLog.Operation.DROP_METALAKE, AuditLog.Operation.fromEvent(dropMetalakeEvent));
    Event dropMetalakeFailureEvent = new DropMetalakeFailureEvent(USER, metalakeIdentifier, null);
    Assertions.assertEquals(
        AuditLog.Operation.DROP_METALAKE, AuditLog.Operation.fromEvent(dropMetalakeFailureEvent));

    Event dropCatalogEvent = new DropCatalogEvent(USER, catalogIdentifier, true);
    Assertions.assertEquals(
        AuditLog.Operation.DROP_CATALOG, AuditLog.Operation.fromEvent(dropCatalogEvent));
    Event dropCatalogFailureEvent =
        new DropCatalogFailureEvent(USER, catalogIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.DROP_CATALOG, AuditLog.Operation.fromEvent(dropCatalogFailureEvent));

    Event dropSchemaEvent = new DropSchemaEvent(USER, schemaIdentifier, true, true);
    Assertions.assertEquals(
        AuditLog.Operation.DROP_SCHEMA, AuditLog.Operation.fromEvent(dropSchemaEvent));
    Event dropSchemaFailureEvent =
        new DropSchemaFailureEvent(USER, schemaIdentifier, new Exception(), true);
    Assertions.assertEquals(
        AuditLog.Operation.DROP_SCHEMA, AuditLog.Operation.fromEvent(dropSchemaFailureEvent));

    Event dropTableEvent = new DropTableEvent(USER, tableIdentifier, true);
    Assertions.assertEquals(
        AuditLog.Operation.DROP_TABLE, AuditLog.Operation.fromEvent(dropTableEvent));
    Event dropTableFailureEvent = new DropTableFailureEvent(USER, tableIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.DROP_TABLE, AuditLog.Operation.fromEvent(dropTableFailureEvent));

    Event dropFilesetEvent = new DropFilesetEvent(USER, filesetIdentifier, true);
    Assertions.assertEquals(
        AuditLog.Operation.DROP_FILESET, AuditLog.Operation.fromEvent(dropFilesetEvent));
    Event dropFilesetFailureEvent =
        new DropFilesetFailureEvent(USER, filesetIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.DROP_FILESET, AuditLog.Operation.fromEvent(dropFilesetFailureEvent));

    Event dropTopicEvent = new DropTopicEvent(USER, topicIdentifier, true);
    Assertions.assertEquals(
        AuditLog.Operation.DROP_TOPIC, AuditLog.Operation.fromEvent(dropTopicEvent));
    Event dropTopicFailureEvent = new DropTopicFailureEvent(USER, topicIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.DROP_TOPIC, AuditLog.Operation.fromEvent(dropTopicFailureEvent));

    Event dropViewEvent = new DropViewEvent(USER, viewIdentifier, true);
    Assertions.assertEquals(
        AuditLog.Operation.DROP_VIEW, AuditLog.Operation.fromEvent(dropViewEvent));
    Event dropViewFailureEvent = new DropViewFailureEvent(USER, viewIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.DROP_VIEW, AuditLog.Operation.fromEvent(dropViewFailureEvent));
  }

  @Test
  public void testPurgeOperation() {
    Event purgeMetalakeEvent =
        new PurgePartitionEvent(USER, metalakeIdentifier, true, PARTITION_NAME);
    Assertions.assertEquals(
        AuditLog.Operation.PURGE_PARTITION, AuditLog.Operation.fromEvent(purgeMetalakeEvent));
    Event purgeMetalakeFailureEvent =
        new PurgePartitionFailureEvent(USER, partitionIdentifier, new Exception(), PARTITION_NAME);
    Assertions.assertEquals(
        AuditLog.Operation.PURGE_PARTITION,
        AuditLog.Operation.fromEvent(purgeMetalakeFailureEvent));

    Event purgeTableEvent = new PurgeTableEvent(USER, tableIdentifier, true);
    Assertions.assertEquals(
        AuditLog.Operation.PURGE_TABLE, AuditLog.Operation.fromEvent(purgeTableEvent));
    Event purgePartitionFailureEvent =
        new PurgePartitionFailureEvent(USER, partitionIdentifier, new Exception(), PARTITION_NAME);
    Assertions.assertEquals(
        AuditLog.Operation.PURGE_PARTITION,
        AuditLog.Operation.fromEvent(purgePartitionFailureEvent));
  }

  @Test
  public void testGetOperation() {
    Event getFileLocationEvent =
        new GetFileLocationEvent(USER, filesetIdentifier, "location", "subPath", new HashMap<>());
    Assertions.assertEquals(
        AuditLog.Operation.GET_FILE_LOCATION, AuditLog.Operation.fromEvent(getFileLocationEvent));
    Event getFilesetFailureEvent =
        new GetFileLocationFailureEvent(USER, filesetIdentifier, "subPath", new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.GET_FILE_LOCATION, AuditLog.Operation.fromEvent(getFilesetFailureEvent));

    Event getPartitionEvent = new GetPartitionEvent(USER, partitionIdentifier, partitionInfo);
    Assertions.assertEquals(
        AuditLog.Operation.GET_PARTITION, AuditLog.Operation.fromEvent(getPartitionEvent));
    Event getPartitionFailureEvent =
        new GetPartitionFailureEvent(USER, partitionIdentifier, new Exception(), PARTITION_NAME);
    Assertions.assertEquals(
        AuditLog.Operation.GET_PARTITION, AuditLog.Operation.fromEvent(getPartitionFailureEvent));
  }

  @Test
  public void testListOperation() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    Event listCatalogEvent = new ListCatalogEvent(USER, namespace, 0);
    Assertions.assertEquals(
        AuditLog.Operation.LIST_CATALOG, AuditLog.Operation.fromEvent(listCatalogEvent));
    Event listCatalogFailureEvent = new ListCatalogFailureEvent(USER, new Exception(), namespace);
    Assertions.assertEquals(
        AuditLog.Operation.LIST_CATALOG, AuditLog.Operation.fromEvent(listCatalogFailureEvent));

    Event listSchemaEvent = new ListSchemaEvent(USER, namespace, 0);
    Assertions.assertEquals(
        AuditLog.Operation.LIST_SCHEMA, AuditLog.Operation.fromEvent(listSchemaEvent));
    Event listSchemaFailureEvent = new ListSchemaFailureEvent(USER, namespace, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.LIST_SCHEMA, AuditLog.Operation.fromEvent(listSchemaFailureEvent));

    Event listTableEvent = new ListTableEvent(USER, namespace, 0);
    Assertions.assertEquals(
        AuditLog.Operation.LIST_TABLE, AuditLog.Operation.fromEvent(listTableEvent));
    Event listTableFailureEvent = new ListTableFailureEvent(USER, namespace, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.LIST_TABLE, AuditLog.Operation.fromEvent(listTableFailureEvent));

    Event listTopicEvent = new ListTopicEvent(USER, namespace, 0);
    Assertions.assertEquals(
        AuditLog.Operation.LIST_TOPIC, AuditLog.Operation.fromEvent(listTopicEvent));
    Event listTopicFailureEvent = new ListTopicFailureEvent(USER, namespace, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.LIST_TOPIC, AuditLog.Operation.fromEvent(listTopicFailureEvent));

    Namespace viewNamespace = Namespace.of("metalake", "catalog", "schema");
    Event listViewEvent = new ListViewEvent(USER, viewNamespace, 0);
    Assertions.assertEquals(
        AuditLog.Operation.LIST_VIEW, AuditLog.Operation.fromEvent(listViewEvent));
    Event listViewFailureEvent = new ListViewFailureEvent(USER, viewNamespace, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.LIST_VIEW, AuditLog.Operation.fromEvent(listViewFailureEvent));

    Event listFilesetEvent = new ListFilesetEvent(USER, namespace, 0);
    Assertions.assertEquals(
        AuditLog.Operation.LIST_FILESET, AuditLog.Operation.fromEvent(listFilesetEvent));
    Event listFilesetFailureEvent = new ListFilesetFailureEvent(USER, namespace, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.LIST_FILESET, AuditLog.Operation.fromEvent(listFilesetFailureEvent));

    Event listPartitionEvent = new ListPartitionEvent(USER, partitionIdentifier, 0);
    Assertions.assertEquals(
        AuditLog.Operation.LIST_PARTITION, AuditLog.Operation.fromEvent(listPartitionEvent));
    Event listPartitionFailureEvent =
        new ListPartitionFailureEvent(USER, partitionIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.LIST_PARTITION, AuditLog.Operation.fromEvent(listPartitionFailureEvent));
  }

  @Test
  public void testLoadOperation() {
    Event loadMetalakeEvent = new LoadMetalakeEvent(USER, metalakeIdentifier, metalakeInfo);
    Assertions.assertEquals(
        AuditLog.Operation.LOAD_METALAKE, AuditLog.Operation.fromEvent(loadMetalakeEvent));
    Event loadMetalakeFailureEvent =
        new LoadMetalakeFailureEvent(USER, metalakeIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.LOAD_METALAKE, AuditLog.Operation.fromEvent(loadMetalakeFailureEvent));

    Event loadCatalogEvent = new LoadCatalogEvent(USER, catalogIdentifier, catalogInfo);
    Assertions.assertEquals(
        AuditLog.Operation.LOAD_CATALOG, AuditLog.Operation.fromEvent(loadCatalogEvent));
    Event loadCatalogFailureEvent =
        new LoadCatalogFailureEvent(USER, catalogIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.LOAD_CATALOG, AuditLog.Operation.fromEvent(loadCatalogFailureEvent));

    Event loadSchemaEvent = new LoadSchemaEvent(USER, schemaIdentifier, schemaInfo);
    Assertions.assertEquals(
        AuditLog.Operation.LOAD_SCHEMA, AuditLog.Operation.fromEvent(loadSchemaEvent));
    Event loadSchemaFailureEvent =
        new LoadSchemaFailureEvent(USER, schemaIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.LOAD_SCHEMA, AuditLog.Operation.fromEvent(loadSchemaFailureEvent));

    Event loadTableEvent = new LoadTableEvent(USER, tableIdentifier, tableInfo);
    Assertions.assertEquals(
        AuditLog.Operation.LOAD_TABLE, AuditLog.Operation.fromEvent(loadTableEvent));
    Event loadTableFailureEvent = new LoadTableFailureEvent(USER, tableIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.LOAD_TABLE, AuditLog.Operation.fromEvent(loadTableFailureEvent));

    Event loadFilesetEvent = new LoadFilesetEvent(USER, filesetIdentifier, filesetInfo);
    Assertions.assertEquals(
        AuditLog.Operation.LOAD_FILESET, AuditLog.Operation.fromEvent(loadFilesetEvent));
    Event loadFilesetFailureEvent =
        new LoadFilesetFailureEvent(USER, filesetIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.LOAD_FILESET, AuditLog.Operation.fromEvent(loadFilesetFailureEvent));

    Event loadTopicEvent = new LoadTopicEvent(USER, topicIdentifier, topicInfo);
    Assertions.assertEquals(
        AuditLog.Operation.LOAD_TOPIC, AuditLog.Operation.fromEvent(loadTopicEvent));
    Event loadTopicFailureEvent = new LoadTopicFailureEvent(USER, topicIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.LOAD_TOPIC, AuditLog.Operation.fromEvent(loadTopicFailureEvent));

    Event loadViewEvent = new LoadViewEvent(USER, viewIdentifier, viewInfo);
    Assertions.assertEquals(
        AuditLog.Operation.LOAD_VIEW, AuditLog.Operation.fromEvent(loadViewEvent));
    Event loadViewFailureEvent = new LoadViewFailureEvent(USER, viewIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.LOAD_VIEW, AuditLog.Operation.fromEvent(loadViewFailureEvent));
  }

  @Test
  public void testEnableDisableOperation() {
    Event enableMetalakeEvent = new EnableMetalakeEvent(USER, metalakeIdentifier);
    Assertions.assertEquals(
        AuditLog.Operation.ENABLE_METALAKE, AuditLog.Operation.fromEvent(enableMetalakeEvent));
    Event enableMetalakeFailureEvent =
        new EnableMetalakeFailureEvent(USER, metalakeIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.ENABLE_METALAKE,
        AuditLog.Operation.fromEvent(enableMetalakeFailureEvent));

    Event disableMetalakeEvent = new DisableMetalakeEvent(USER, metalakeIdentifier);
    Assertions.assertEquals(
        AuditLog.Operation.DISABLE_METALAKE, AuditLog.Operation.fromEvent(disableMetalakeEvent));
    Event disableMetalakeFailureEvent =
        new DisableMetalakeFailureEvent(USER, metalakeIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.DISABLE_METALAKE,
        AuditLog.Operation.fromEvent(disableMetalakeFailureEvent));

    Event enableCatalogEvent = new EnableCatalogEvent(USER, catalogIdentifier);
    Assertions.assertEquals(
        AuditLog.Operation.ENABLE_CATALOG, AuditLog.Operation.fromEvent(enableCatalogEvent));
    Event enableCatalogFailureEvent =
        new EnableCatalogFailureEvent(USER, catalogIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.ENABLE_CATALOG, AuditLog.Operation.fromEvent(enableCatalogFailureEvent));

    Event disableCatalogEvent = new DisableCatalogEvent(USER, catalogIdentifier);
    Assertions.assertEquals(
        AuditLog.Operation.DISABLE_CATALOG, AuditLog.Operation.fromEvent(disableCatalogEvent));
    Event disableCatalogFailureEvent =
        new DisableCatalogFailureEvent(USER, catalogIdentifier, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.DISABLE_CATALOG,
        AuditLog.Operation.fromEvent(disableCatalogFailureEvent));
  }

  @Test
  public void testAuthorizationDenialOperation() {
    Event authzDenialEvent =
        new AuthorizationDenialFailureEvent(
            USER, catalogIdentifier, "listCatalogs", "CATALOG:LIST");
    Assertions.assertEquals(
        AuditLog.Operation.AUTHORIZATION_DENIAL, AuditLog.Operation.fromEvent(authzDenialEvent));

    // null identifier variant (denial before resource resolution)
    Event authzDenialNullIdentifier =
        new AuthorizationDenialFailureEvent(USER, null, "loadTable", "TABLE:LOAD");
    Assertions.assertEquals(
        AuditLog.Operation.AUTHORIZATION_DENIAL,
        AuditLog.Operation.fromEvent(authzDenialNullIdentifier));
  }

  @Test
  public void testHttpRequestFailureEventMapsToUnknownOperation() {
    // HttpRequestFailureEvent has no specific business operation — it always maps to UNKNOWN
    Event httpFailureEvent =
        new HttpRequestFailureEvent(
            USER, "1.2.3.4", "GET", "/api/metalakes", 401, EventSource.GRAVITINO_SERVER);
    Assertions.assertEquals(
        AuditLog.Operation.UNKNOWN_OPERATION, AuditLog.Operation.fromEvent(httpFailureEvent));

    // Verify the same holds for other status codes (400, 403, 404, 500)
    for (int status : new int[] {400, 403, 404, 500}) {
      Event e =
          new HttpRequestFailureEvent(
              USER, "1.2.3.4", "POST", "/api/metalakes", status, EventSource.GRAVITINO_SERVER);
      Assertions.assertEquals(
          AuditLog.Operation.UNKNOWN_OPERATION,
          AuditLog.Operation.fromEvent(e),
          "HttpRequestFailureEvent with status " + status + " must map to UNKNOWN_OPERATION");
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testSimpleFormatterHandlesServerEvents() {
    // Smoke-test the v1 SimpleFormatter with the two new server-event types to confirm it
    // does not NPE on their identifier/customInfo and maps to the correct Operation values.
    SimpleFormatter formatter = new SimpleFormatter();

    HttpRequestFailureEvent httpEvent =
        new HttpRequestFailureEvent(
            USER, "1.2.3.4", "GET", "/api/metalakes", 401, EventSource.GRAVITINO_SERVER);
    SimpleAuditLog httpLog = formatter.format(httpEvent);
    Assertions.assertEquals(AuditLog.Operation.UNKNOWN_OPERATION, httpLog.operation());
    Assertions.assertEquals(AuditLog.Status.FAILURE, httpLog.status());
    Assertions.assertNull(httpLog.identifier());

    AuthorizationDenialFailureEvent authzEvent =
        new AuthorizationDenialFailureEvent(USER, catalogIdentifier, "loadTable", "TABLE:LOAD");
    SimpleAuditLog authzLog = formatter.format(authzEvent);
    Assertions.assertEquals(AuditLog.Operation.AUTHORIZATION_DENIAL, authzLog.operation());
    Assertions.assertEquals(AuditLog.Status.FAILURE, authzLog.status());
    Assertions.assertEquals(catalogIdentifier.toString(), authzLog.identifier());
  }

  @Test
  public void testExistsOperation() {
    Event partitionExistsEvent =
        new PartitionExistsEvent(USER, partitionIdentifier, true, partitionIdentifier.name());
    Assertions.assertEquals(
        AuditLog.Operation.PARTITION_EXIST, AuditLog.Operation.fromEvent(partitionExistsEvent));

    Event partitionExistsFailureEvent =
        new PartitionExistsEvent(USER, partitionIdentifier, true, partitionIdentifier.name());
    Assertions.assertEquals(
        AuditLog.Operation.PARTITION_EXIST,
        AuditLog.Operation.fromEvent(partitionExistsFailureEvent));
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

  private NameIdentifier mockViewIdentifier() {
    return NameIdentifier.of("metalake", "catalog", "schema", "view");
  }

  private ViewInfo mockViewInfo() {
    return new ViewInfo(
        "view",
        new Column[] {Column.of("a", Types.IntegerType.get())},
        "comment",
        new Representation[] {
          SQLRepresentation.builder().withDialect("trino").withSql("SELECT 1").build()
        },
        "dc",
        "ds",
        ImmutableMap.of("a", "b"),
        null);
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
