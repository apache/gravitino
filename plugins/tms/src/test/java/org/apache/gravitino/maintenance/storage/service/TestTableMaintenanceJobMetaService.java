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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

class TestTableMaintenanceJobMetaService extends TestJDBCBackend {

  private TableMaintenanceJobMetaService service;

  @BeforeEach
  void setUpService() {
    service = new TableMaintenanceJobMetaService();
  }

  @TestTemplate
  void testInsertBindSampleAndFinishLifecycle() {
    long rowId = service.insertInFlightJob(10L, 20L);
    Assertions.assertTrue(rowId > 0);
    long jobId = 999L;
    service.bindJobId(rowId, jobId);
    service.recordBeforeSnapshot(jobId, 100L);
    Assertions.assertDoesNotThrow(
        () -> service.markFinished(jobId, 200L, System.currentTimeMillis()));
  }
}
