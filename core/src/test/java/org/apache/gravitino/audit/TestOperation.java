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

import static org.mockito.ArgumentMatchers.any;

import java.util.HashMap;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SchemaChange;
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
import org.apache.gravitino.listener.api.info.partitions.PartitionInfo;
import org.apache.gravitino.messaging.TopicChange;
import org.apache.gravitino.rel.TableChange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOperation {

  @Test
  public void testCreateOperation() {
    Event createMetalakeEvent =
        new CreateMetalakeEvent(
            any(String.class), any(NameIdentifier.class), any(MetalakeInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createMetalakeEvent), AuditLog.Operation.CREATE_METALAKE);
    Event createMetalakeFaileEvent =
        new CreateMetalakeFailureEvent(
            any(String.class), any(NameIdentifier.class), new Exception(), any(MetalakeInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createMetalakeFaileEvent), AuditLog.Operation.CREATE_METALAKE);

    Event createCatalogEvent =
        new CreateCatalogEvent(
            any(String.class), any(NameIdentifier.class), any(CatalogInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createCatalogEvent), AuditLog.Operation.CREATE_CATALOG);
    Event CreateCatalogFailureEvent =
        new CreateCatalogFailureEvent(
            any(String.class), any(NameIdentifier.class), new Exception(), any(CatalogInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(CreateCatalogFailureEvent), AuditLog.Operation.CREATE_CATALOG);

    Event createSchemaEvent =
        new CreateSchemaEvent(any(String.class), any(NameIdentifier.class), any(SchemaInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createSchemaEvent), AuditLog.Operation.CREATE_SCHEMA);
    Event createSchemaFailureEvent =
        new CreateSchemaFailureEvent(
            any(String.class), any(NameIdentifier.class), new Exception(), any(SchemaInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createSchemaFailureEvent), AuditLog.Operation.CREATE_SCHEMA);

    Event createTableEvent =
        new CreateTableEvent(any(String.class), any(NameIdentifier.class), any(TableInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createTableEvent), AuditLog.Operation.CREATE_TABLE);
    Event createTableFailureEvent =
        new CreateTableFailureEvent(
            any(String.class), any(NameIdentifier.class), new Exception(), any(TableInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createTableFailureEvent), AuditLog.Operation.CREATE_TABLE);

    Event createFilesetEvent =
        new CreateFilesetEvent(
            any(String.class), any(NameIdentifier.class), any(FilesetInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createFilesetEvent), AuditLog.Operation.CREATE_FILESET);
    Event createFilesetFailureEvent =
        new CreateFilesetFailureEvent(
            any(String.class), any(NameIdentifier.class), new Exception(), any(FilesetInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createFilesetFailureEvent), AuditLog.Operation.CREATE_FILESET);

    Event createTopicEvent =
        new CreateTopicEvent(any(String.class), any(NameIdentifier.class), any(TopicInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createTopicEvent), AuditLog.Operation.CREATE_TOPIC);
    Event createTopicFailureEvent =
        new CreateTopicFailureEvent(
            any(String.class), any(NameIdentifier.class), new Exception(), any(TopicInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(createTopicFailureEvent), AuditLog.Operation.CREATE_TOPIC);
  }

  @Test
  public void testAlterOperation() {
    Event alterMetalakeEvent =
        new AlterMetalakeEvent(
            any(String.class),
            any(NameIdentifier.class),
            new MetalakeChange[] {},
            any(MetalakeInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterMetalakeEvent), AuditLog.Operation.ALTER_METALAKE);
    Event alterMetalakeFailureEvent =
        new AlterMetalakeFailureEvent(
            any(String.class), any(NameIdentifier.class), new Exception(), new MetalakeChange[] {});
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterMetalakeFailureEvent), AuditLog.Operation.ALTER_METALAKE);

    Event alterCatalogEvent =
        new AlterCatalogEvent(
            any(String.class),
            any(NameIdentifier.class),
            new CatalogChange[] {},
            any(CatalogInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterCatalogEvent), AuditLog.Operation.ALTER_CATALOG);
    Event alterCatalogFailureEvent =
        new AlterCatalogFailureEvent(
            any(String.class), any(NameIdentifier.class), new Exception(), new CatalogChange[] {});
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterCatalogFailureEvent), AuditLog.Operation.ALTER_CATALOG);

    Event alterSchemaEvent =
        new AlterSchemaEvent(
            any(String.class),
            any(NameIdentifier.class),
            new SchemaChange[] {},
            any(SchemaInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterSchemaEvent), AuditLog.Operation.ALTER_SCHEMA);
    Event alterSchemaFailureEvent =
        new AlterSchemaFailureEvent(
            any(String.class), any(NameIdentifier.class), new Exception(), new SchemaChange[] {});
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterSchemaFailureEvent), AuditLog.Operation.ALTER_SCHEMA);

    Event alterTableEvent =
        new AlterTableEvent(
            any(String.class),
            any(NameIdentifier.class),
            new TableChange[] {},
            any(TableInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterTableEvent), AuditLog.Operation.ALTER_TABLE);
    Event alterTableFailureEvent =
        new AlterTableFailureEvent(
            any(String.class), any(NameIdentifier.class), new Exception(), new TableChange[] {});
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterTableFailureEvent), AuditLog.Operation.ALTER_TABLE);

    Event alterFilesetEvent =
        new AlterFilesetEvent(
            any(String.class),
            any(NameIdentifier.class),
            new FilesetChange[] {},
            any(FilesetInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterFilesetEvent), AuditLog.Operation.ALTER_FILESET);
    Event alterFilesetFailureEvent =
        new AlterFilesetFailureEvent(
            any(String.class), any(NameIdentifier.class), new Exception(), new FilesetChange[] {});
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterFilesetFailureEvent), AuditLog.Operation.ALTER_FILESET);

    Event alterTopicEvent =
        new AlterTopicEvent(
            any(String.class),
            any(NameIdentifier.class),
            new TopicChange[] {},
            any(TopicInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterTopicEvent), AuditLog.Operation.ALTER_TOPIC);
    Event alterTopicFailureEvent =
        new AlterTopicFailureEvent(
            any(String.class), any(NameIdentifier.class), new Exception(), new TopicChange[] {});
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(alterTopicFailureEvent), AuditLog.Operation.ALTER_TOPIC);
  }

  @Test
  public void testDropOperation() {
    Event dropMetalakeEvent =
        new DropMetalakeEvent(any(String.class), any(NameIdentifier.class), true);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropMetalakeEvent), AuditLog.Operation.DROP_METALAKE);
    Event dropMetalakeFailureEvent =
        new DropMetalakeFailureEvent(any(String.class), any(NameIdentifier.class), null);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropMetalakeFailureEvent), AuditLog.Operation.DROP_METALAKE);

    Event dropCatalogEvent =
        new DropCatalogEvent(any(String.class), any(NameIdentifier.class), true);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropCatalogEvent), AuditLog.Operation.DROP_CATALOG);
    Event dropCatalogFailureEvent =
        new DropCatalogFailureEvent(any(String.class), any(NameIdentifier.class), new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropCatalogFailureEvent), AuditLog.Operation.DROP_CATALOG);

    Event dropSchemaEvent =
        new DropSchemaEvent(any(String.class), any(NameIdentifier.class), true, true);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropSchemaEvent), AuditLog.Operation.DROP_SCHEMA);
    Event dropSchemaFailureEvent =
        new DropSchemaFailureEvent(
            any(String.class), any(NameIdentifier.class), new Exception(), true);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropSchemaFailureEvent), AuditLog.Operation.DROP_SCHEMA);

    Event dropTableEvent = new DropTableEvent(any(String.class), any(NameIdentifier.class), true);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropTableEvent), AuditLog.Operation.DROP_TABLE);
    Event dropTableFailureEvent =
        new DropTableFailureEvent(any(String.class), any(NameIdentifier.class), new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropTableFailureEvent), AuditLog.Operation.DROP_TABLE);

    Event dropFilesetEvent =
        new DropFilesetEvent(any(String.class), any(NameIdentifier.class), true);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropFilesetEvent), AuditLog.Operation.DROP_FILESET);
    Event dropFilesetFailureEvent =
        new DropFilesetFailureEvent(any(String.class), any(NameIdentifier.class), new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropFilesetFailureEvent), AuditLog.Operation.DROP_FILESET);

    Event dropTopicEvent = new DropTopicEvent(any(String.class), any(NameIdentifier.class), true);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropTopicEvent), AuditLog.Operation.DROP_TOPIC);
    Event dropTopicFailureEvent =
        new DropTopicFailureEvent(any(String.class), any(NameIdentifier.class), new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(dropTopicFailureEvent), AuditLog.Operation.DROP_TOPIC);
  }

  @Test
  public void testPurgeOperation() {
    Event purgeMetalakeEvent =
        new PurgePartitionEvent(
            any(String.class), any(NameIdentifier.class), true, any(String.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(purgeMetalakeEvent), AuditLog.Operation.PURGE_PARTITION);
    Event purgeMetalakeFailureEvent =
        new PurgePartitionFailureEvent(
            any(String.class), any(NameIdentifier.class), new Exception(), any(String.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(purgeMetalakeFailureEvent),
        AuditLog.Operation.PURGE_PARTITION);

    Event purgeTableEvent = new PurgeTableEvent(any(String.class), any(NameIdentifier.class), true);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(purgeTableEvent), AuditLog.Operation.PURGE_TABLE);
    Event purgePartitionFailureEvent =
        new PurgePartitionFailureEvent(
            any(String.class), any(NameIdentifier.class), new Exception(), any(String.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(purgePartitionFailureEvent),
        AuditLog.Operation.PURGE_PARTITION);
  }

  @Test
  public void testGetOperation() {
    Event getFileLocationEvent =
        new GetFileLocationEvent(
            any(String.class),
            any(NameIdentifier.class),
            any(String.class),
            any(String.class),
            new HashMap<>());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(getFileLocationEvent), AuditLog.Operation.GET_FILE_LOCATION);
    Event getFilesetFailureEvent =
        new GetFileLocationFailureEvent(
            any(String.class), any(NameIdentifier.class), any(String.class), new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(getFilesetFailureEvent), AuditLog.Operation.GET_FILE_LOCATION);

    Event getPartitionEvent =
        new GetPartitionEvent(
            any(String.class), any(NameIdentifier.class), any(PartitionInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(getPartitionEvent), AuditLog.Operation.GET_PARTITION);
    Event getPartitionFailureEvent =
        new GetPartitionFailureEvent(
            any(String.class), any(NameIdentifier.class), new Exception(), any(String.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(getPartitionFailureEvent), AuditLog.Operation.GET_PARTITION);
  }

  @Test
  public void testListOperation() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    Event listCatalogEvent = new ListCatalogEvent(any(String.class), namespace);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listCatalogEvent), AuditLog.Operation.LIST_CATALOG);
    Event listCatalogFailureEvent =
        new ListCatalogFailureEvent(any(String.class), new Exception(), namespace);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listCatalogFailureEvent), AuditLog.Operation.LIST_CATALOG);

    Event listSchemaEvent = new ListSchemaEvent(any(String.class), namespace);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listSchemaEvent), AuditLog.Operation.LIST_SCHEMA);
    Event listSchemaFailureEvent =
        new ListSchemaFailureEvent(any(String.class), namespace, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listSchemaFailureEvent), AuditLog.Operation.LIST_SCHEMA);

    Event listTableEvent = new ListTableEvent(any(String.class), namespace);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listTableEvent), AuditLog.Operation.LIST_TABLE);
    Event listTableFailureEvent =
        new ListTableFailureEvent(any(String.class), namespace, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listTableFailureEvent), AuditLog.Operation.LIST_TABLE);

    Event listTopicEvent = new ListTopicEvent(any(String.class), namespace);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listTopicEvent), AuditLog.Operation.LIST_TOPIC);
    Event listTopicFailureEvent =
        new ListTopicFailureEvent(any(String.class), namespace, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listTopicFailureEvent), AuditLog.Operation.LIST_TOPIC);

    Event listFilesetEvent = new ListFilesetEvent(any(String.class), namespace);
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listFilesetEvent), AuditLog.Operation.LIST_FILESET);
    Event listFilesetFailureEvent =
        new ListFilesetFailureEvent(any(String.class), namespace, new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listFilesetFailureEvent), AuditLog.Operation.LIST_FILESET);

    Event listPartitionEvent = new ListPartitionEvent(any(String.class), any(NameIdentifier.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listPartitionEvent), AuditLog.Operation.LIST_PARTITION);
    Event listPartitionFailureEvent =
        new ListPartitionFailureEvent(
            any(String.class), any(NameIdentifier.class), new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(listPartitionFailureEvent), AuditLog.Operation.LIST_PARTITION);
  }

  @Test
  public void testLoadOperation() {
    Event loadMetalakeEvent =
        new LoadMetalakeEvent(
            any(String.class), any(NameIdentifier.class), any(MetalakeInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadMetalakeEvent), AuditLog.Operation.LOAD_METALAKE);
    Event loadMetalakeFailureEvent =
        new LoadMetalakeFailureEvent(any(String.class), any(NameIdentifier.class), new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadMetalakeFailureEvent), AuditLog.Operation.LOAD_METALAKE);

    Event loadCatalogEvent =
        new LoadCatalogEvent(any(String.class), any(NameIdentifier.class), any(CatalogInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadCatalogEvent), AuditLog.Operation.LOAD_CATALOG);
    Event loadCatalogFailureEvent =
        new LoadCatalogFailureEvent(any(String.class), any(NameIdentifier.class), new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadCatalogFailureEvent), AuditLog.Operation.LOAD_CATALOG);

    Event loadSchemaEvent =
        new LoadSchemaEvent(any(String.class), any(NameIdentifier.class), any(SchemaInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadSchemaEvent), AuditLog.Operation.LOAD_SCHEMA);
    Event loadSchemaFailureEvent =
        new LoadSchemaFailureEvent(any(String.class), any(NameIdentifier.class), new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadSchemaFailureEvent), AuditLog.Operation.LOAD_SCHEMA);

    Event loadTableEvent =
        new LoadTableEvent(any(String.class), any(NameIdentifier.class), any(TableInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadTableEvent), AuditLog.Operation.LOAD_TABLE);
    Event loadTableFailureEvent =
        new LoadTableFailureEvent(any(String.class), any(NameIdentifier.class), new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadTableFailureEvent), AuditLog.Operation.LOAD_TABLE);

    Event loadFilesetEvent =
        new LoadFilesetEvent(any(String.class), any(NameIdentifier.class), any(FilesetInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadFilesetEvent), AuditLog.Operation.LOAD_FILESET);
    Event loadFilesetFailureEvent =
        new LoadFilesetFailureEvent(any(String.class), any(NameIdentifier.class), new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadFilesetFailureEvent), AuditLog.Operation.LOAD_FILESET);

    Event loadTopicEvent =
        new LoadTopicEvent(any(String.class), any(NameIdentifier.class), any(TopicInfo.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadTopicEvent), AuditLog.Operation.LOAD_TOPIC);
    Event loadTopicFailureEvent =
        new LoadTopicFailureEvent(any(String.class), any(NameIdentifier.class), new Exception());
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(loadTopicFailureEvent), AuditLog.Operation.LOAD_TOPIC);
  }

  @Test
  public void testExistsOperation() {
    Event partitionExistsEvent =
        new PartitionExistsEvent(
            any(String.class), any(NameIdentifier.class), true, any(String.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(partitionExistsEvent), AuditLog.Operation.PARTITION_EXIST);

    Event partitionExistsFailureEvent =
        new PartitionExistsEvent(
            any(String.class), any(NameIdentifier.class), true, any(String.class));
    Assertions.assertEquals(
        AuditLog.Operation.fromEvent(partitionExistsFailureEvent),
        AuditLog.Operation.PARTITION_EXIST);
  }
}
