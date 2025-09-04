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
package org.apache.gravitino.stats;

import java.util.List;

/**
 * PartitionDropStatistics represents the statistics related to dropping partitions in a data
 * source. It is used to manage and track the statistics that are relevant when partitions are
 * dropped.
 */
public interface PartitionStatisticsDrop {

  /**
   * Returns the name of the partition for which these statistics are applicable.
   *
   * @return the name of the partition
   */
  String partitionName();

  /**
   * Returns the names of the statistics that are relevant to the partition being dropped.
   *
   * @return a list of statistic names
   */
  List<String> statisticNames();
}
