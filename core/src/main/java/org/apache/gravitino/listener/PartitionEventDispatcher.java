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

package org.apache.gravitino.listener;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.PartitionDispatcher;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.listener.api.event.AddPartitionEvent;
import org.apache.gravitino.listener.api.event.AddPartitionFailureEvent;
import org.apache.gravitino.listener.api.event.DropPartitionEvent;
import org.apache.gravitino.listener.api.event.DropPartitionFailureEvent;
import org.apache.gravitino.listener.api.event.GetPartitionEvent;
import org.apache.gravitino.listener.api.event.GetPartitionFailureEvent;
import org.apache.gravitino.listener.api.event.ListPartitionEvent;
import org.apache.gravitino.listener.api.event.ListPartitionFailureEvent;
import org.apache.gravitino.listener.api.event.ListPartitionNamesEvent;
import org.apache.gravitino.listener.api.event.ListPartitionNamesFailureEvent;
import org.apache.gravitino.listener.api.event.PartitionExistsEvent;
import org.apache.gravitino.listener.api.event.PartitionExistsFailureEvent;
import org.apache.gravitino.listener.api.event.PurgePartitionEvent;
import org.apache.gravitino.listener.api.event.PurgePartitionFailureEvent;
import org.apache.gravitino.listener.api.info.partitions.PartitionInfo;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code PartitionEventDispatcher} is a decorator for {@link PartitionDispatcher} that not only
 * delegates partition operations to the underlying partition dispatcher but also dispatches
 * corresponding events to an {@link org.apache.gravitino.listener.EventBus} after each operation is
 * completed. This allows for event-driven workflows or monitoring of partition operations.
 */
public class PartitionEventDispatcher implements PartitionDispatcher {
  private final EventBus eventBus;
  private final PartitionDispatcher dispatcher;

  /**
   * Constructs a PartitionEventDispatcher with a specified EventBus and PartitionDispatcher.
   *
   * @param eventBus The EventBus to which events will be dispatched.
   * @param dispatcher The underlying {@link PartitionDispatcher} that will perform the actual
   *     partition operations.
   */
  public PartitionEventDispatcher(EventBus eventBus, PartitionDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  @Override
  public Partition addPartition(NameIdentifier ident, Partition partition)
      throws NoSuchPartitionException, PartitionAlreadyExistsException {
    try {
      Partition newPartition = dispatcher.addPartition(ident, partition);
      eventBus.dispatchEvent(
          new AddPartitionEvent(
              PrincipalUtils.getCurrentUserName(), ident, PartitionInfo.of(newPartition)));
      return newPartition;
    } catch (Exception e) {
      PartitionInfo createdPartitionInfo = PartitionInfo.of(partition);
      eventBus.dispatchEvent(
          new AddPartitionFailureEvent(
              PrincipalUtils.getCurrentUserName(), ident, e, createdPartitionInfo));
      throw e;
    }
  }

  @Override
  public Partition getPartition(NameIdentifier ident, String partitionName)
      throws NoSuchPartitionException {
    try {
      Partition partition = dispatcher.getPartition(ident, partitionName);
      eventBus.dispatchEvent(
          new GetPartitionEvent(
              PrincipalUtils.getCurrentUserName(), ident, PartitionInfo.of(partition)));
      return partition;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new GetPartitionFailureEvent(
              PrincipalUtils.getCurrentUserName(), ident, e, partitionName));
      throw e;
    }
  }

  @Override
  public boolean dropPartition(NameIdentifier ident, String partitionName) {
    try {
      boolean isExists = dispatcher.dropPartition(ident, partitionName);
      eventBus.dispatchEvent(
          new DropPartitionEvent(
              PrincipalUtils.getCurrentUserName(), ident, isExists, partitionName));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new DropPartitionFailureEvent(
              PrincipalUtils.getCurrentUserName(), ident, e, partitionName));
      throw e;
    }
  }

  @Override
  public Partition[] listPartitions(NameIdentifier ident) {
    try {
      Partition[] listPartitions = dispatcher.listPartitions(ident);
      eventBus.dispatchEvent(new ListPartitionEvent(PrincipalUtils.getCurrentUserName(), ident));
      return listPartitions;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListPartitionFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }

  @Override
  public String[] listPartitionNames(NameIdentifier ident) {
    try {
      String[] listPartitionNames = dispatcher.listPartitionNames(ident);
      eventBus.dispatchEvent(
          new ListPartitionNamesEvent(PrincipalUtils.getCurrentUserName(), ident));
      return listPartitionNames;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListPartitionNamesFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }

  @Override
  public boolean partitionExists(NameIdentifier ident, String partitionName) {
    try {
      boolean isExists = dispatcher.partitionExists(ident, partitionName);
      eventBus.dispatchEvent(
          new PartitionExistsEvent(
              PrincipalUtils.getCurrentUserName(), ident, isExists, partitionName));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new PartitionExistsFailureEvent(
              PrincipalUtils.getCurrentUserName(), ident, e, partitionName));
      throw e;
    }
  }

  @Override
  public boolean purgePartition(NameIdentifier ident, String partitionName) {
    try {
      boolean isExists = dispatcher.purgePartition(ident, partitionName);
      eventBus.dispatchEvent(
          new PurgePartitionEvent(
              PrincipalUtils.getCurrentUserName(), ident, isExists, partitionName));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new PurgePartitionFailureEvent(
              PrincipalUtils.getCurrentUserName(), ident, e, partitionName));
      throw e;
    }
  }
}
