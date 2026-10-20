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
package org.apache.gravitino.maintenance.scheduler.cron;

import java.util.Map;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.meta.JobEntity;

/**
 * Submits a Gravitino Job for a TMS cron-job callback (§5.2.2).
 *
 * <p>Production wiring uses {@link GravitinoEnv#jobOperationDispatcher()}; tests inject a fake.
 */
@FunctionalInterface
public interface MaintenanceJobSubmitter {

  /**
   * Runs a job template and returns the persisted job-run id for occupancy bind.
   *
   * @param metalake metalake name
   * @param jobTemplateName built-in or registered job template name
   * @param jobConf runtime job configuration (policy jobOptions + auth overlay)
   * @return {@link JobEntity#id()}
   */
  long runJob(String metalake, String jobTemplateName, Map<String, String> jobConf);

  /**
   * @return submitter that delegates to {@link GravitinoEnv#jobOperationDispatcher()}
   */
  static MaintenanceJobSubmitter fromEnv() {
    return (metalake, jobTemplateName, jobConf) -> {
      JobEntity job =
          GravitinoEnv.getInstance()
              .jobOperationDispatcher()
              .runJob(metalake, jobTemplateName, jobConf);
      return job.id();
    };
  }
}
