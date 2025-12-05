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
package org.apache.gravitino.listener.api.event.stats;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.stats.PartitionRange;

/** Event fired when listing partition statistics. */
@DeveloperApi
public class ListPartitionStatisticsEvent extends StatisticsEvent {
  private final PartitionRange partitionRange;

  /**
   * Creates a new ListPartitionStatisticsEvent.
   *
   * @param user the user performing the operation
   * @param identifier the identifier of the metadata object
   * @param partitionRange the partition range for which statistics are being listed
   */
  public ListPartitionStatisticsEvent(
      String user, NameIdentifier identifier, PartitionRange partitionRange) {
    super(user, identifier);
    this.partitionRange = partitionRange;
  }

  /**
   * Gets the partition range for which statistics are being listed.
   *
   * @return the partition range
   */
  public PartitionRange partitionRange() {
    return partitionRange;
  }

  @Override
  public OperationType operationType() {
    return OperationType.LIST_PARTITION_STATISTICS;
  }
}
