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

package org.apache.gravitino.listener.api.event;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.time.LocalDate;
import java.util.Arrays;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.PartitionDispatcher;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.PartitionEventDispatcher;
import org.apache.gravitino.listener.api.info.partitions.IdentityPartitionInfo;
import org.apache.gravitino.listener.api.info.partitions.ListPartitionInfo;
import org.apache.gravitino.listener.api.info.partitions.PartitionInfo;
import org.apache.gravitino.listener.api.info.partitions.RangePartitionInfo;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.partitions.IdentityPartition;
import org.apache.gravitino.rel.partitions.ListPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.partitions.RangePartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestPartitionEvent {
  private PartitionDispatcher dispatcher;
  private PartitionDispatcher failureDispatcher;
  private DummyEventListener dummyEventListener;
  private Partition partition;

  @BeforeAll
  void init() {
    this.partition = mockPartition();
    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Arrays.asList(dummyEventListener));
    PartitionDispatcher partitionDispatcher = mockPartitionDispatcher();
    this.dispatcher = new PartitionEventDispatcher(eventBus, partitionDispatcher);
    PartitionDispatcher partitionExceptionDispatcher = mockExceptionPartitionDispatcher();
    this.failureDispatcher = new PartitionEventDispatcher(eventBus, partitionExceptionDispatcher);
  }

  @Test
  void testCreatePartitionInfo() {
    Partition partition =
        Partitions.range("p0", Literals.NULL, Literals.integerLiteral(6), Maps.newHashMap());
    PartitionInfo partitionInfo = PartitionInfo.of(partition);
    checkPartitionInfo(partitionInfo, partition);

    partition =
        Partitions.list(
            "p202204_California",
            new Literal[][] {
              {
                Literals.dateLiteral(LocalDate.parse("2022-04-01")),
                Literals.stringLiteral("Los Angeles")
              },
              {
                Literals.dateLiteral(LocalDate.parse("2022-04-01")),
                Literals.stringLiteral("San Francisco")
              }
            },
            Maps.newHashMap());
    partitionInfo = PartitionInfo.of(partition);
    checkPartitionInfo(partitionInfo, partition);

    partition =
        Partitions.identity(
            "dt=2008-08-08/country=us",
            new String[][] {{"dt"}, {"country"}},
            new Literal[] {
              Literals.dateLiteral(LocalDate.parse("2008-08-08")), Literals.stringLiteral("us")
            },
            ImmutableMap.of("location", "/user/hive/warehouse/tpch_flat_orc_2.db/orders"));
    partitionInfo = PartitionInfo.of(partition);
    checkPartitionInfo(partitionInfo, partition);

    Assertions.assertThrowsExactly(GravitinoRuntimeException.class, () -> PartitionInfo.of(null));
  }

  @Test
  void testAddPartitionEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "table");
    dispatcher.addPartition(identifier, partition);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AddPartitionEvent.class, event.getClass());
    PartitionInfo partitionInfo = ((AddPartitionEvent) event).createdPartitionInfo();
    checkPartitionInfo(partitionInfo, partition);
  }

  @Test
  void testDropPartitionEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "table");
    dispatcher.dropPartition(identifier, partition.name());
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropPartitionEvent.class, event.getClass());
    Assertions.assertEquals(false, ((DropPartitionEvent) event).isExists());
  }

  @Test
  void testPartitionExistsEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "table");
    dispatcher.partitionExists(identifier, partition.name());
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(PartitionExistsEvent.class, event.getClass());
    Assertions.assertEquals(false, ((PartitionExistsEvent) event).isExists());
  }

  @Test
  void testListPartitionEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "table");
    dispatcher.listPartitions(identifier);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(ListPartitionEvent.class, event.getClass());
    Assertions.assertEquals(identifier, ((ListPartitionEvent) event).identifier());
  }

  @Test
  void testListPartitionNamesEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "table");
    dispatcher.listPartitionNames(identifier);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(ListPartitionNamesEvent.class, event.getClass());
    Assertions.assertEquals(identifier, ((ListPartitionNamesEvent) event).identifier());
  }

  @Test
  void testPurgePartitionEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "table");
    dispatcher.purgePartition(identifier, partition.name());
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(PurgePartitionEvent.class, event.getClass());
    Assertions.assertEquals(identifier, ((PurgePartitionEvent) event).identifier());
  }

  @Test
  void testAddPartitionFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "table");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.addPartition(identifier, partition));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AddPartitionFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((AddPartitionFailureEvent) event).exception().getClass());
    checkPartitionInfo(((AddPartitionFailureEvent) event).createdPartitionInfo(), partition);
    Assertions.assertEquals(identifier, event.identifier());
  }

  @Test
  void testDropPartitionFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "table");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.dropPartition(identifier, partition.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(DropPartitionFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((DropPartitionFailureEvent) event).exception().getClass());
    Assertions.assertEquals(identifier, event.identifier());
  }

  @Test
  void testPartitionExistsFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "table");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.partitionExists(identifier, partition.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(PartitionExistsFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((PartitionExistsFailureEvent) event).exception().getClass());
    Assertions.assertEquals(identifier, event.identifier());
  }

  @Test
  void testListPartitionFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "table");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listPartitions(identifier));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListPartitionFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((ListPartitionFailureEvent) event).exception().getClass());
    Assertions.assertEquals(identifier, ((ListPartitionFailureEvent) event).identifier());
  }

  @Test
  void testListPartitionNamesFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "table");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listPartitionNames(identifier));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListPartitionNamesFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((ListPartitionNamesFailureEvent) event).exception().getClass());
    Assertions.assertEquals(identifier, ((ListPartitionNamesFailureEvent) event).identifier());
  }

  @Test
  void testPurgePartitionFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "table");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.purgePartition(identifier, partition.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(PurgePartitionFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((PurgePartitionFailureEvent) event).exception().getClass());
    Assertions.assertEquals(identifier, event.identifier());
  }

  private void checkPartitionInfo(PartitionInfo partitionInfo, Partition partition) {
    Assertions.assertEquals(partition.name(), partitionInfo.name());
    Assertions.assertEquals(partition.properties(), partitionInfo.properties());

    if (partitionInfo instanceof ListPartitionInfo) {
      Assertions.assertEquals(
          ((ListPartition) partition).lists(), ((ListPartitionInfo) partitionInfo).lists());
    } else if (partitionInfo instanceof IdentityPartitionInfo) {
      Assertions.assertEquals(
          ((IdentityPartition) partition).fieldNames(),
          ((IdentityPartitionInfo) partitionInfo).fieldNames());
      Assertions.assertEquals(
          ((IdentityPartition) partition).values(),
          ((IdentityPartitionInfo) partitionInfo).values());
    } else if (partitionInfo instanceof RangePartitionInfo) {
      Assertions.assertEquals(
          ((RangePartition) partition).upper(), ((RangePartitionInfo) partitionInfo).upper());
      Assertions.assertEquals(
          ((RangePartition) partition).lower(), ((RangePartitionInfo) partitionInfo).lower());
    }
  }

  private Partition mockPartition() {
    Partition partition =
        Partitions.range("p0", Literals.NULL, Literals.integerLiteral(6), Maps.newHashMap());
    return partition;
  }

  private PartitionDispatcher mockPartitionDispatcher() {
    PartitionDispatcher dispatcher = mock(PartitionDispatcher.class);
    when(dispatcher.addPartition(any(NameIdentifier.class), any(Partition.class)))
        .thenReturn(partition);
    when(dispatcher.getPartition(any(NameIdentifier.class), any(String.class)))
        .thenReturn(partition);
    when(dispatcher.listPartitionNames(any(NameIdentifier.class))).thenReturn(null);
    when(dispatcher.listPartitions(any(NameIdentifier.class))).thenReturn(null);
    return dispatcher;
  }

  private PartitionDispatcher mockExceptionPartitionDispatcher() {
    PartitionDispatcher dispatcher =
        mock(
            PartitionDispatcher.class,
            invocation -> {
              throw new GravitinoRuntimeException("Exception for all methods");
            });
    return dispatcher;
  }
}
