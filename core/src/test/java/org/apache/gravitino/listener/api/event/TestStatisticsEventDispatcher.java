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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.StatisticEventDispatcher;
import org.apache.gravitino.listener.api.event.stats.DropPartitionStatisticsEvent;
import org.apache.gravitino.listener.api.event.stats.DropPartitionStatisticsFailureEvent;
import org.apache.gravitino.listener.api.event.stats.DropPartitionStatisticsPreEvent;
import org.apache.gravitino.listener.api.event.stats.DropStatisticsEvent;
import org.apache.gravitino.listener.api.event.stats.DropStatisticsFailureEvent;
import org.apache.gravitino.listener.api.event.stats.DropStatisticsPreEvent;
import org.apache.gravitino.listener.api.event.stats.ListPartitionStatisticsEvent;
import org.apache.gravitino.listener.api.event.stats.ListPartitionStatisticsPreEvent;
import org.apache.gravitino.listener.api.event.stats.ListStatisticsEvent;
import org.apache.gravitino.listener.api.event.stats.ListStatisticsFailureEvent;
import org.apache.gravitino.listener.api.event.stats.ListStatisticsPreEvent;
import org.apache.gravitino.listener.api.event.stats.UpdatePartitionStatisticsEvent;
import org.apache.gravitino.listener.api.event.stats.UpdatePartitionStatisticsFailureEvent;
import org.apache.gravitino.listener.api.event.stats.UpdatePartitionStatisticsPreEvent;
import org.apache.gravitino.listener.api.event.stats.UpdateStatisticsEvent;
import org.apache.gravitino.listener.api.event.stats.UpdateStatisticsFailureEvent;
import org.apache.gravitino.listener.api.event.stats.UpdateStatisticsPreEvent;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatisticsDrop;
import org.apache.gravitino.stats.PartitionStatisticsModification;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticDispatcher;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestStatisticsEventDispatcher {
  private StatisticEventDispatcher dispatcher;
  private StatisticEventDispatcher failureDispatcher;

  private DummyEventListener dummyEventListener;

  @BeforeAll
  public void setup() {
    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Collections.singletonList(dummyEventListener));
    StatisticDispatcher statisticDispatcher = mockStatisticDispatcher();
    this.dispatcher = new StatisticEventDispatcher(eventBus, statisticDispatcher);
    StatisticDispatcher statisticExceptionDispatcher = mockExceptionStatisticDispatcher();
    this.failureDispatcher = new StatisticEventDispatcher(eventBus, statisticExceptionDispatcher);
  }

  @Test
  public void testListStatisticsEvent() {
    dispatcher.listStatistics(
        "metalake",
        MetadataObjects.of(
            Lists.newArrayList("catalog", "db", "table"), MetadataObject.Type.TABLE));

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "db", "table"), preEvent.identifier());
    Assertions.assertEquals(ListStatisticsPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.LIST_STATISTICS, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "db", "table"), event.identifier());
    Assertions.assertEquals(ListStatisticsEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.LIST_STATISTICS, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
  }

  @Test
  public void testListStatisticsFailureEvent() {
    Assertions.assertThrows(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.listStatistics(
                "metalake",
                MetadataObjects.of(
                    Lists.newArrayList("catalog", "db", "table"), MetadataObject.Type.TABLE)));

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "db", "table"), event.identifier());
    Assertions.assertEquals(OperationType.LIST_STATISTICS, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(ListStatisticsFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((ListStatisticsFailureEvent) event).exception().getClass());
  }

  @Test
  public void testListPartitionStatisticsEvent() {
    dispatcher.listPartitionStatistics(
        "metalake",
        MetadataObjects.of(Lists.newArrayList("catalog", "db", "table"), MetadataObject.Type.TABLE),
        PartitionRange.upTo("p1", PartitionRange.BoundType.CLOSED));
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "db", "table"), preEvent.identifier());
    Assertions.assertEquals(ListPartitionStatisticsPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.LIST_PARTITION_STATISTICS, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "db", "table"), event.identifier());
    Assertions.assertEquals(ListPartitionStatisticsEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.LIST_PARTITION_STATISTICS, event.operationType());
  }

  @Test
  public void testUpdateStatisticsEvent() {
    Map<String, StatisticValue<?>> stats = Maps.newHashMap();
    stats.put("stat1", StatisticValues.longValue(100L));

    dispatcher.updateStatistics(
        "metalake",
        MetadataObjects.of(Lists.newArrayList("catalog", "db", "table"), MetadataObject.Type.TABLE),
        stats);
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "db", "table"), preEvent.identifier());
    Assertions.assertEquals(UpdateStatisticsPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.UPDATE_STATISTICS, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(
        100L, ((UpdateStatisticsPreEvent) preEvent).statistics().get("stat1").value());

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "db", "table"), event.identifier());
    Assertions.assertEquals(UpdateStatisticsEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.UPDATE_STATISTICS, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(
        100L, ((UpdateStatisticsEvent) event).statistics().get("stat1").value());
  }

  @Test
  public void testUpdateStatisticsFailureEvent() {
    Map<String, StatisticValue<?>> stats = Maps.newHashMap();
    stats.put("stat1", StatisticValues.longValue(100L));

    Assertions.assertThrows(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.updateStatistics(
                "metalake",
                MetadataObjects.of(
                    Lists.newArrayList("catalog", "db", "table"), MetadataObject.Type.TABLE),
                stats));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "db", "table"), event.identifier());
    Assertions.assertEquals(OperationType.UPDATE_STATISTICS, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(UpdateStatisticsFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((UpdateStatisticsFailureEvent) event).exception().getClass());
    Assertions.assertEquals(
        100L, ((UpdateStatisticsFailureEvent) event).statistics().get("stat1").value());
  }

  @Test
  public void testUpdatePartitionStatisticsEvent() {
    Map<String, StatisticValue<?>> stats = Maps.newHashMap();
    stats.put("stat1", StatisticValues.longValue(100L));

    PartitionStatisticsUpdate update = PartitionStatisticsModification.update("p1", stats);
    dispatcher.updatePartitionStatistics(
        "metalake",
        MetadataObjects.of(Lists.newArrayList("catalog", "db", "table"), MetadataObject.Type.TABLE),
        Lists.newArrayList(update));

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "db", "table"), preEvent.identifier());
    Assertions.assertEquals(UpdatePartitionStatisticsPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.UPDATE_PARTITION_STATISTICS, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(
        1, ((UpdatePartitionStatisticsPreEvent) preEvent).partitionStatisticsUpdates().size());
    Assertions.assertEquals(
        "p1",
        ((UpdatePartitionStatisticsPreEvent) preEvent)
            .partitionStatisticsUpdates()
            .get(0)
            .partitionName());
    Assertions.assertEquals(
        100L,
        ((UpdatePartitionStatisticsPreEvent) preEvent)
            .partitionStatisticsUpdates()
            .get(0)
            .statistics()
            .get("stat1")
            .value());

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "db", "table"), event.identifier());
    Assertions.assertEquals(UpdatePartitionStatisticsEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.UPDATE_PARTITION_STATISTICS, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(
        1, ((UpdatePartitionStatisticsEvent) event).partitionStatisticsUpdates().size());
    Assertions.assertEquals(
        "p1",
        ((UpdatePartitionStatisticsEvent) event)
            .partitionStatisticsUpdates()
            .get(0)
            .partitionName());
    Assertions.assertEquals(
        100L,
        ((UpdatePartitionStatisticsEvent) event)
            .partitionStatisticsUpdates()
            .get(0)
            .statistics()
            .get("stat1")
            .value());
  }

  @Test
  public void testUpdatePartitionStatisticsFailureEvent() {
    Map<String, StatisticValue<?>> stats = Maps.newHashMap();
    stats.put("stat1", StatisticValues.longValue(100L));

    PartitionStatisticsUpdate update = PartitionStatisticsModification.update("p1", stats);
    Assertions.assertThrows(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.updatePartitionStatistics(
                "metalake",
                MetadataObjects.of(
                    Lists.newArrayList("catalog", "db", "table"), MetadataObject.Type.TABLE),
                Lists.newArrayList(update)));

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "db", "table"), event.identifier());
    Assertions.assertEquals(OperationType.UPDATE_PARTITION_STATISTICS, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(UpdatePartitionStatisticsFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((UpdatePartitionStatisticsFailureEvent) event).exception().getClass());
    Assertions.assertEquals(
        1, ((UpdatePartitionStatisticsFailureEvent) event).partitionStatisticsUpdates().size());
    Assertions.assertEquals(
        "p1",
        ((UpdatePartitionStatisticsFailureEvent) event)
            .partitionStatisticsUpdates()
            .get(0)
            .partitionName());
    Assertions.assertEquals(
        100L,
        ((UpdatePartitionStatisticsFailureEvent) event)
            .partitionStatisticsUpdates()
            .get(0)
            .statistics()
            .get("stat1")
            .value());
  }

  @Test
  public void testDropStatisticsEvent() {
    dispatcher.dropStatistics(
        "metalake",
        MetadataObjects.of(Lists.newArrayList("catalog", "db", "table"), MetadataObject.Type.TABLE),
        Lists.newArrayList("stat1", "stat2"));

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "db", "table"), preEvent.identifier());
    Assertions.assertEquals(DropStatisticsPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.DROP_STATISTICS, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(2, ((DropStatisticsPreEvent) preEvent).statisticNames().size());
    Assertions.assertTrue(((DropStatisticsPreEvent) preEvent).statisticNames().contains("stat1"));
    Assertions.assertTrue(((DropStatisticsPreEvent) preEvent).statisticNames().contains("stat2"));

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "db", "table"), event.identifier());
    Assertions.assertEquals(DropStatisticsEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.DROP_STATISTICS, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(2, ((DropStatisticsEvent) event).statisticNames().size());
    Assertions.assertTrue(((DropStatisticsEvent) event).statisticNames().contains("stat1"));
    Assertions.assertTrue(((DropStatisticsEvent) event).statisticNames().contains("stat2"));
  }

  @Test
  public void testDropStatisticsFailureEvent() {
    Assertions.assertThrows(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.dropStatistics(
                "metalake",
                MetadataObjects.of(
                    Lists.newArrayList("catalog", "db", "table"), MetadataObject.Type.TABLE),
                Lists.newArrayList("stat1", "stat2")));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "db", "table"), event.identifier());
    Assertions.assertEquals(OperationType.DROP_STATISTICS, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(DropStatisticsFailureEvent.class, event.getClass());
    Assertions.assertEquals(2, ((DropStatisticsFailureEvent) event).statisticNames().size());
    Assertions.assertTrue(((DropStatisticsFailureEvent) event).statisticNames().contains("stat1"));
    Assertions.assertTrue(((DropStatisticsFailureEvent) event).statisticNames().contains("stat2"));
  }

  @Test
  public void testDropPartitionStatisticsEvent() {
    PartitionStatisticsDrop drop1 =
        PartitionStatisticsModification.drop("p1", Lists.newArrayList("stat1", "stat2"));
    PartitionStatisticsDrop drop2 =
        PartitionStatisticsModification.drop("p2", Lists.newArrayList("stat3"));

    dispatcher.dropPartitionStatistics(
        "metalake",
        MetadataObjects.of(Lists.newArrayList("catalog", "db", "table"), MetadataObject.Type.TABLE),
        Lists.newArrayList(drop1, drop2));

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "db", "table"), preEvent.identifier());
    Assertions.assertEquals(DropPartitionStatisticsPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.DROP_PARTITION_STATISTICS, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(
        2, ((DropPartitionStatisticsPreEvent) preEvent).partitionStatisticsDrops().size());
    Assertions.assertEquals(
        "p1",
        ((DropPartitionStatisticsPreEvent) preEvent)
            .partitionStatisticsDrops()
            .get(0)
            .partitionName());
    Assertions.assertEquals(
        "p2",
        ((DropPartitionStatisticsPreEvent) preEvent)
            .partitionStatisticsDrops()
            .get(1)
            .partitionName());
    Assertions.assertEquals(
        2,
        ((DropPartitionStatisticsPreEvent) preEvent)
            .partitionStatisticsDrops()
            .get(0)
            .statisticNames()
            .size());
    Assertions.assertEquals(
        1,
        ((DropPartitionStatisticsPreEvent) preEvent)
            .partitionStatisticsDrops()
            .get(1)
            .statisticNames()
            .size());
    Assertions.assertEquals(
        "stat1",
        ((DropPartitionStatisticsPreEvent) preEvent)
            .partitionStatisticsDrops()
            .get(0)
            .statisticNames()
            .get(0));
    Assertions.assertEquals(
        "stat2",
        ((DropPartitionStatisticsPreEvent) preEvent)
            .partitionStatisticsDrops()
            .get(0)
            .statisticNames()
            .get(1));

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "db", "table"), event.identifier());
    Assertions.assertEquals(DropPartitionStatisticsEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.DROP_PARTITION_STATISTICS, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(
        2, ((DropPartitionStatisticsEvent) event).partitionStatisticsDrops().size());
    Assertions.assertEquals(
        "p1",
        ((DropPartitionStatisticsEvent) event).partitionStatisticsDrops().get(0).partitionName());
    Assertions.assertEquals(
        "p2",
        ((DropPartitionStatisticsEvent) event).partitionStatisticsDrops().get(1).partitionName());
    Assertions.assertEquals(
        2,
        ((DropPartitionStatisticsEvent) event)
            .partitionStatisticsDrops()
            .get(0)
            .statisticNames()
            .size());
    Assertions.assertEquals(
        1,
        ((DropPartitionStatisticsEvent) event)
            .partitionStatisticsDrops()
            .get(1)
            .statisticNames()
            .size());
    Assertions.assertEquals(
        "stat1",
        ((DropPartitionStatisticsEvent) event)
            .partitionStatisticsDrops()
            .get(0)
            .statisticNames()
            .get(0));
    Assertions.assertEquals(
        "stat2",
        ((DropPartitionStatisticsEvent) event)
            .partitionStatisticsDrops()
            .get(0)
            .statisticNames()
            .get(1));
  }

  @Test
  public void testDropPartitionStatisticsFailureEvent() {
    PartitionStatisticsDrop drop1 =
        PartitionStatisticsModification.drop("p1", Lists.newArrayList("stat1", "stat2"));
    PartitionStatisticsDrop drop2 =
        PartitionStatisticsModification.drop("p2", Lists.newArrayList("stat3"));

    Assertions.assertThrows(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.dropPartitionStatistics(
                "metalake",
                MetadataObjects.of(
                    Lists.newArrayList("catalog", "db", "table"), MetadataObject.Type.TABLE),
                Lists.newArrayList(drop1, drop2)));

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "db", "table"), event.identifier());
    Assertions.assertEquals(OperationType.DROP_PARTITION_STATISTICS, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(DropPartitionStatisticsFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        2, ((DropPartitionStatisticsFailureEvent) event).partitionStatisticsDrops().size());
    Assertions.assertEquals(
        "p1",
        ((DropPartitionStatisticsFailureEvent) event)
            .partitionStatisticsDrops()
            .get(0)
            .partitionName());
    Assertions.assertEquals(
        "p2",
        ((DropPartitionStatisticsFailureEvent) event)
            .partitionStatisticsDrops()
            .get(1)
            .partitionName());
    Assertions.assertEquals(
        2,
        ((DropPartitionStatisticsFailureEvent) event)
            .partitionStatisticsDrops()
            .get(0)
            .statisticNames()
            .size());
    Assertions.assertEquals(
        1,
        ((DropPartitionStatisticsFailureEvent) event)
            .partitionStatisticsDrops()
            .get(1)
            .statisticNames()
            .size());
    Assertions.assertEquals(
        "stat1",
        ((DropPartitionStatisticsFailureEvent) event)
            .partitionStatisticsDrops()
            .get(0)
            .statisticNames()
            .get(0));
    Assertions.assertEquals(
        "stat2",
        ((DropPartitionStatisticsFailureEvent) event)
            .partitionStatisticsDrops()
            .get(0)
            .statisticNames()
            .get(1));
  }

  @AfterAll
  public void tearDown() throws Exception {
    dummyEventListener.clear();
  }

  private StatisticDispatcher mockExceptionStatisticDispatcher() {
    return mock(
        StatisticDispatcher.class,
        invocation -> {
          throw new GravitinoRuntimeException("Exception for all methods");
        });
  }

  private StatisticDispatcher mockStatisticDispatcher() {
    StatisticDispatcher dispatcher = mock(StatisticDispatcher.class);
    when(dispatcher.dropPartitionStatistics(any(), any(), any())).thenReturn(true);
    when(dispatcher.dropStatistics(any(), any(), any())).thenReturn(true);
    when(dispatcher.listPartitionStatistics(any(), any(), any()))
        .thenReturn(Collections.emptyList());
    when(dispatcher.listStatistics(any(), any())).thenReturn(Collections.emptyList());

    return dispatcher;
  }
}
