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

package org.apache.gravitino.maintenance.optimizer.api.common;

import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Strategy definition for partitioned tables.
 *
 * <p>Partition strategies provide additional configuration for aggregating partition scores and
 * selecting the number of partitions to evaluate.
 */
@DeveloperApi
public interface PartitionStrategy extends Strategy {

  int DEFAULT_MAX_PARTITION_NUM = 100;

  /**
   * Partition table score aggregation mode.
   *
   * <p>Defaults to {@link ScoreMode#AVG}.
   *
   * @return score mode enum
   */
  default ScoreMode partitionTableScoreMode() {
    return ScoreMode.AVG;
  }

  /**
   * Maximum number of partitions to include in a table evaluation.
   *
   * <p>Defaults to {@value #DEFAULT_MAX_PARTITION_NUM}.
   *
   * @return max partition number
   */
  default int maxPartitionNum() {
    return DEFAULT_MAX_PARTITION_NUM;
  }

  /** Partition table score aggregation mode. */
  enum ScoreMode {
    /** Average score of all partitions. */
    AVG,
    /** Maximum score of all partitions. */
    MAX,
    /** Sum score of all partitions. */
    SUM,
  }
}
