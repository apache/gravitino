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
import org.apache.gravitino.stats.PartitionStatisticsDrop;

/**
 * MetadataObjectStatisticsDrop represents a collection of statistics drops for a specific
 * MetadataObject.
 */
public class MetadataObjectStatisticsDrop {

  private final MetadataObject metadataObject;

  private final List<PartitionStatisticsDrop> drops;

  /**
   * Creates a new instance of MetadataObjectStatisticsDrop.
   *
   * @param metadataObject the MetadataObject for which these statistics drops are applicable
   * @param drops a list of PartitionStatisticsDrop objects representing the statistics drops for
   *     the
   * @return a new instance of MetadataObjectStatisticsDrop
   */
  public static MetadataObjectStatisticsDrop of(
      MetadataObject metadataObject, List<PartitionStatisticsDrop> drops) {
    return new MetadataObjectStatisticsDrop(metadataObject, drops);
  }

  private MetadataObjectStatisticsDrop(
      MetadataObject metadataObject, List<PartitionStatisticsDrop> drops) {
    this.metadataObject = metadataObject;
    this.drops = drops;
  }

  /**
   * Returns the MetadataObject for which these statistics drops are applicable.
   *
   * @return the MetadataObject
   */
  public MetadataObject metadataObject() {
    return metadataObject;
  }

  /**
   * Returns the list of PartitionStatisticsDrop objects representing the statistics drops for the
   * MetadataObject.
   *
   * @return a list of PartitionStatisticsDrop objects
   */
  public List<PartitionStatisticsDrop> drops() {
    return drops;
  }
}
