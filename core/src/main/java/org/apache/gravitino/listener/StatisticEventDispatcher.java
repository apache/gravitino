/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.listener;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.listener.api.event.stats.DropPartitionStatisticsEvent;
import org.apache.gravitino.listener.api.event.stats.DropPartitionStatisticsFailureEvent;
import org.apache.gravitino.listener.api.event.stats.DropPartitionStatisticsPreEvent;
import org.apache.gravitino.listener.api.event.stats.DropStatisticsEvent;
import org.apache.gravitino.listener.api.event.stats.DropStatisticsFailureEvent;
import org.apache.gravitino.listener.api.event.stats.DropStatisticsPreEvent;
import org.apache.gravitino.listener.api.event.stats.ListPartitionStatisticsEvent;
import org.apache.gravitino.listener.api.event.stats.ListPartitionStatisticsFailureEvent;
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
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.PartitionStatisticsDrop;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.stats.StatisticDispatcher;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * StatisticEventManager is a decorator for StatisticDispatcher that dispatches events to an
 * EventBus.
 */
public class StatisticEventDispatcher implements StatisticDispatcher {
  private final EventBus eventBus;
  private final StatisticDispatcher dispatcher;

  public StatisticEventDispatcher(EventBus eventBus, StatisticDispatcher dispatcher) {
    this.dispatcher = dispatcher;
    this.eventBus = eventBus;
  }

  @Override
  public List<Statistic> listStatistics(String metalake, MetadataObject metadataObject) {
    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    String user = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new ListStatisticsPreEvent(user, identifier));

    try {
      List<Statistic> statistics = dispatcher.listStatistics(metalake, metadataObject);
      eventBus.dispatchEvent(new ListStatisticsEvent(user, identifier));
      return statistics;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListStatisticsFailureEvent(user, identifier, e));
      throw e;
    }
  }

  @Override
  public void updateStatistics(
      String metalake, MetadataObject metadataObject, Map<String, StatisticValue<?>> statistics) {
    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    String user = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new UpdateStatisticsPreEvent(user, identifier, statistics));

    try {
      dispatcher.updateStatistics(metalake, metadataObject, statistics);
      eventBus.dispatchEvent(new UpdateStatisticsEvent(user, identifier, statistics));
    } catch (Exception e) {
      eventBus.dispatchEvent(new UpdateStatisticsFailureEvent(user, identifier, e, statistics));
      throw e;
    }
  }

  @Override
  public boolean dropStatistics(
      String metalake, MetadataObject metadataObject, List<String> statistics) {
    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    String user = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new DropStatisticsPreEvent(user, identifier, statistics));

    try {
      boolean result = dispatcher.dropStatistics(metalake, metadataObject, statistics);
      eventBus.dispatchEvent(new DropStatisticsEvent(user, identifier, statistics));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(new DropStatisticsFailureEvent(user, identifier, e, statistics));
      throw e;
    }
  }

  @Override
  public boolean dropPartitionStatistics(
      String metalake,
      MetadataObject metadataObject,
      List<PartitionStatisticsDrop> partitionStatistics) {
    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    String user = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(
        new DropPartitionStatisticsPreEvent(user, identifier, partitionStatistics));

    try {
      boolean result =
          dispatcher.dropPartitionStatistics(metalake, metadataObject, partitionStatistics);
      eventBus.dispatchEvent(
          new DropPartitionStatisticsEvent(user, identifier, partitionStatistics));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new DropPartitionStatisticsFailureEvent(user, identifier, e, partitionStatistics));
      throw e;
    }
  }

  @Override
  public void updatePartitionStatistics(
      String metalake,
      MetadataObject metadataObject,
      List<PartitionStatisticsUpdate> partitionStatistics) {
    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    String user = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(
        new UpdatePartitionStatisticsPreEvent(user, identifier, partitionStatistics));

    try {
      dispatcher.updatePartitionStatistics(metalake, metadataObject, partitionStatistics);
      eventBus.dispatchEvent(
          new UpdatePartitionStatisticsEvent(user, identifier, partitionStatistics));
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new UpdatePartitionStatisticsFailureEvent(user, identifier, e, partitionStatistics));
      throw e;
    }
  }

  @Override
  public List<PartitionStatistics> listPartitionStatistics(
      String metalake, MetadataObject metadataObject, PartitionRange range) {
    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    String user = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new ListPartitionStatisticsPreEvent(user, identifier, range));

    try {
      List<PartitionStatistics> partitionStatistics =
          dispatcher.listPartitionStatistics(metalake, metadataObject, range);
      eventBus.dispatchEvent(new ListPartitionStatisticsEvent(user, identifier, range));
      return partitionStatistics;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListPartitionStatisticsFailureEvent(user, identifier, e, range));
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    dispatcher.close();
  }
}
