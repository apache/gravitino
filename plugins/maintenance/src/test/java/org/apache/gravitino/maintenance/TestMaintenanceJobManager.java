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
package org.apache.gravitino.maintenance;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.job.JobOperationDispatcher;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceJobTemplates;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestMaintenanceJobManager {

  private static final String METALAKE = "ml";
  private static final String SUBMITTER = "tms";

  @Test
  void constructorRequiresJobSubmitterWhenAuthorizationEnabled() {
    JobOperationDispatcher jobs = mock(JobOperationDispatcher.class);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new MaintenanceJobManager(jobs, true, null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new MaintenanceJobManager(jobs, true, "  "));
    Assertions.assertDoesNotThrow(() -> new MaintenanceJobManager(jobs, false, null));
  }

  @Test
  void runJobWithoutDoAsWhenAuthorizationDisabled() {
    JobOperationDispatcher jobs = mock(JobOperationDispatcher.class);
    JobEntity job = mock(JobEntity.class);
    when(jobs.runJob(
            eq(METALAKE), eq(MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE), anyMap()))
        .thenReturn(job);

    MaintenanceJobManager manager = new MaintenanceJobManager(jobs, false, null);
    Map<String, String> conf = new HashMap<>();
    Assertions.assertSame(job, manager.runIcebergMaintenanceJob(METALAKE, conf));
    verify(jobs)
        .runJob(eq(METALAKE), eq(MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE), eq(conf));
  }

  @Test
  void runJobAsConfiguredSubmitterWhenAuthorizationEnabled() {
    JobOperationDispatcher jobs = mock(JobOperationDispatcher.class);
    JobEntity job = mock(JobEntity.class);

    AtomicReference<String> principalDuringRun = new AtomicReference<>();
    when(jobs.runJob(
            eq(METALAKE), eq(MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE), anyMap()))
        .thenAnswer(
            invocation -> {
              principalDuringRun.set(PrincipalUtils.getCurrentUserName());
              return job;
            });

    MaintenanceJobManager manager = new MaintenanceJobManager(jobs, true, SUBMITTER);
    Map<String, String> conf = new HashMap<>();
    Assertions.assertSame(job, manager.runIcebergMaintenanceJob(METALAKE, conf));
    Assertions.assertEquals(SUBMITTER, principalDuringRun.get());
  }
}
