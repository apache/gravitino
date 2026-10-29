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

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobOperationDispatcher;
import org.apache.gravitino.meta.JobEntity;

/**
 * Submits and cancels Gravitino Jobs for TMS cron / commit control-plane pipelines.
 *
 * <p>Production wiring uses {@link GravitinoEnv#jobOperationDispatcher()}; tests inject a fake
 * dispatcher or mock this service.
 */
public class MaintenanceJobSubmitService {

  private final JobOperationDispatcher jobOperationDispatcher;

  /**
   * Production wiring.
   *
   * @return service bound to {@link GravitinoEnv#jobOperationDispatcher()}
   */
  public static MaintenanceJobSubmitService create() {
    return new MaintenanceJobSubmitService(GravitinoEnv.getInstance().jobOperationDispatcher());
  }

  /**
   * Creates a service with an explicit job dispatcher.
   *
   * @param jobOperationDispatcher Gravitino job run / cancel entry point
   */
  public MaintenanceJobSubmitService(JobOperationDispatcher jobOperationDispatcher) {
    this.jobOperationDispatcher =
        Preconditions.checkNotNull(jobOperationDispatcher, "jobOperationDispatcher");
  }

  /**
   * Runs a job template and returns the persisted job-run id for occupancy bind.
   *
   * @param metalake metalake name
   * @param jobTemplateName built-in or registered job template name
   * @param jobConf runtime job configuration (policy jobOptions + auth overlay)
   * @return {@link JobEntity#id()}
   */
  public long runJob(String metalake, String jobTemplateName, Map<String, String> jobConf) {
    JobEntity job = jobOperationDispatcher.runJob(metalake, jobTemplateName, jobConf);
    return job.id();
  }

  /**
   * Best-effort cancel after a successful {@link #runJob} when occupancy bind fails.
   *
   * @param metalake metalake name
   * @param jobId numeric Gravitino job run id
   */
  public void cancelJob(String metalake, long jobId) {
    jobOperationDispatcher.cancelJob(metalake, JobHandle.JOB_ID_PREFIX + jobId);
  }
}
