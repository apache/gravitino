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

package org.apache.gravitino.maintenance.storage.service;

import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

class TestTableMaintenanceJobMetaService extends TestJDBCBackend {

  @TestTemplate
  void testInsertSampleFinishAndDeleteByJobId() {
    TableMaintenanceJobMetaService jobService = TableMaintenanceJobMetaService.getInstance();
    TableSnapshotMetricsMetaService metricsService = TableSnapshotMetricsMetaService.getInstance();

    long jobId = 999L;
    jobService.insertJob(jobId, 10L, 20L);
    jobService.recordBeforeSnapshot(jobId, 100L);
    metricsService.insertMetrics(10L, 20L, 100L, "{\"mse\":1}");
    jobService.markFinished(jobId, 200L, System.currentTimeMillis());
    metricsService.insertMetrics(10L, 20L, 200L, "{\"mse\":2}");

    jobService.deleteByJobId(jobId);

    Assertions.assertNull(metricsService.getMetrics(10L, 20L, 100L));
    Assertions.assertNull(metricsService.getMetrics(10L, 20L, 200L));
  }
}
