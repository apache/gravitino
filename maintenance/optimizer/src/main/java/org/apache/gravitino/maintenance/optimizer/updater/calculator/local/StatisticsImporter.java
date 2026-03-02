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

package org.apache.gravitino.maintenance.optimizer.updater.calculator.local;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.TableAndPartitionStatistics;

/** Importer abstraction for table, partition, and job statistics. */
public interface StatisticsImporter {
  /**
   * Reads table-level and partition-level statistics for a single table.
   *
   * @param tableIdentifier table identifier
   * @return table statistics bundle for the table, empty when absent
   */
  TableAndPartitionStatistics readTableStatistics(NameIdentifier tableIdentifier);

  /**
   * Reads table-level and partition-level statistics for all tables in the source.
   *
   * @return map keyed by table identifier
   */
  Map<NameIdentifier, TableAndPartitionStatistics> bulkReadAllTableStatistics();

  /**
   * Reads job-level statistics for a single job.
   *
   * @param jobIdentifier job identifier
   * @return job statistics for the job, empty when absent
   */
  List<StatisticEntry<?>> readJobStatistics(NameIdentifier jobIdentifier);

  /**
   * Reads job-level statistics for all jobs in the source.
   *
   * @return map keyed by job identifier
   */
  Map<NameIdentifier, List<StatisticEntry<?>>> bulkReadAllJobStatistics();
}
