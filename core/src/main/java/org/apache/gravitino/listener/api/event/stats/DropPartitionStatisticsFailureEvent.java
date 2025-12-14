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

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.stats.PartitionStatisticsDrop;

/** Event fired when there is a failure in dropping partition statistics. */
@DeveloperApi
public class DropPartitionStatisticsFailureEvent extends StatisticsFailureEvent {
  private final List<PartitionStatisticsDrop> partitionStatisticsDrops;

  /**
   * Constructor for creating a drop partition statistics failure event.
   *
   * @param user the user performing the operation
   * @param identifier the identifier of the table
   * @param cause the exception that caused the failure
   * @param partitionStatisticsDrops the partition statistics drops to be applied
   */
  public DropPartitionStatisticsFailureEvent(
      String user,
      NameIdentifier identifier,
      Exception cause,
      List<PartitionStatisticsDrop> partitionStatisticsDrops) {
    super(user, identifier, cause);
    this.partitionStatisticsDrops = partitionStatisticsDrops;
  }

  /**
   * Gets the partition statistics drops associated with this event.
   *
   * @return the partition statistics drops
   */
  public List<PartitionStatisticsDrop> partitionStatisticsDrops() {
    return partitionStatisticsDrops;
  }

  @Override
  public OperationType operationType() {
    return OperationType.DROP_PARTITION_STATISTICS;
  }
}
