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
package org.apache.gravitino.stats.storage;

import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;

/**
 * MetadataObjectStatisticsUpdate represents a collection of statistics updates for a specific
 * MetadataObject.
 */
public class MetadataObjectStatisticsUpdate {

  private final MetadataObject metadataObject;

  private final List<PartitionStatisticsUpdate> partitionUpdates;

  /**
   * Creates a new instance of MetadataObjectStatisticsUpdate.
   *
   * @param metadataObject the MetadataObject for which these statistics updates are applicable
   * @param partitionUpdates a list of PartitionStatisticsUpdate objects representing the statistics
   *     updates
   * @return a new instance of MetadataObjectStatisticsUpdate
   */
  public static MetadataObjectStatisticsUpdate of(
      MetadataObject metadataObject, List<PartitionStatisticsUpdate> partitionUpdates) {
    return new MetadataObjectStatisticsUpdate(metadataObject, partitionUpdates);
  }

  private MetadataObjectStatisticsUpdate(
      MetadataObject metadataObject, List<PartitionStatisticsUpdate> partitionUpdates) {
    this.metadataObject = metadataObject;
    this.partitionUpdates = partitionUpdates;
  }

  /**
   * Returns the MetadataObject for which these statistics updates are applicable.
   *
   * @return the MetadataObject
   */
  public MetadataObject metadataObject() {
    return metadataObject;
  }

  /**
   * Returns the list of PartitionStatisticsUpdate objects representing the statistics updates
   *
   * @return a list of PartitionStatisticsUpdate objects
   */
  public List<PartitionStatisticsUpdate> partitionUpdates() {
    return partitionUpdates;
  }
}
