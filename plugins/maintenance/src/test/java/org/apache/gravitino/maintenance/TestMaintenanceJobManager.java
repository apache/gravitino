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

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobOperationDispatcher;
import org.apache.gravitino.maintenance.occupancy.OccupancyClaim;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.meta.JobEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestMaintenanceJobManager {

  @Test
  void testSubmitAndBindSuccessInjectsActionIds() throws Exception {
    Env env = Env.withJob(999L);
    List<OccupancyClaim> claims = List.of(new OccupancyClaim(7L, 1_000L));
    Map<String, String> conf = new HashMap<>();
    Assertions.assertEquals(Optional.of(999L), env.mgr.submitAndBind("ml", conf, claims));
    Assertions.assertEquals("7", conf.get(MaintenanceJobTemplates.ACTION_IDS_KEY));
    verify(env.actions).bindJobIds(claims, 999L);
    verify(env.dispatcher, never()).cancelJob(anyString(), anyString());

    List<OccupancyClaim> batch = List.of(new OccupancyClaim(1L, 10L), new OccupancyClaim(2L, 20L));
    when(env.job.id()).thenReturn(42L);
    conf = new HashMap<>();
    Assertions.assertEquals(Optional.of(42L), env.mgr.submitAndBind("ml", conf, batch));
    Assertions.assertEquals("1,2", conf.get(MaintenanceJobTemplates.ACTION_IDS_KEY));
    verify(env.actions).bindJobIds(batch, 42L);
  }

  @Test
  void testRunJobFailureReleasesWithoutCancel() throws Exception {
    Env env = Env.open();
    when(env.dispatcher.runJob(anyString(), anyString(), anyMap()))
        .thenThrow(new RuntimeException("submit failed"));
    List<OccupancyClaim> claims = List.of(new OccupancyClaim(8L, 2_000L));

    Assertions.assertTrue(env.mgr.submitAndBind("ml", new HashMap<>(), claims).isEmpty());
    verify(env.actions).deleteClaims(claims);
    verify(env.actions, never()).bindJobIds(anyList(), anyLong());
    verify(env.dispatcher, never()).cancelJob(anyString(), anyString());
  }

  @Test
  void testBindFailureCancelsOrphanAndReleasesClaims() throws Exception {
    Env env = Env.withJob(777L);
    List<OccupancyClaim> claims = List.of(new OccupancyClaim(9L, 3_000L));
    doThrow(new RuntimeException("bind failed")).when(env.actions).bindJobIds(claims, 777L);
    Assertions.assertTrue(env.mgr.submitAndBind("ml", new HashMap<>(), claims).isEmpty());
    verify(env.dispatcher).cancelJob("ml", JobHandle.JOB_ID_PREFIX + 777L);
    verify(env.actions).deleteClaims(claims);

    List<OccupancyClaim> batch = List.of(new OccupancyClaim(1L, 1L), new OccupancyClaim(2L, 2L));
    when(env.job.id()).thenReturn(55L);
    doThrow(new RuntimeException("bind failed")).when(env.actions).bindJobIds(batch, 55L);
    Assertions.assertTrue(env.mgr.submitAndBind("ml", new HashMap<>(), batch).isEmpty());
    verify(env.dispatcher).cancelJob("ml", JobHandle.JOB_ID_PREFIX + 55L);
    verify(env.actions).deleteClaims(batch);
  }

  private static final class Env {
    final JobOperationDispatcher dispatcher = mock(JobOperationDispatcher.class);
    final TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    final JobEntity job = mock(JobEntity.class);
    final MaintenanceJobManager mgr = new MaintenanceJobManager(dispatcher, actions);

    static Env open() {
      return new Env();
    }

    static Env withJob(long id) throws Exception {
      Env env = new Env();
      when(env.job.id()).thenReturn(id);
      when(env.dispatcher.runJob(anyString(), anyString(), anyMap())).thenReturn(env.job);
      return env;
    }
  }
}
