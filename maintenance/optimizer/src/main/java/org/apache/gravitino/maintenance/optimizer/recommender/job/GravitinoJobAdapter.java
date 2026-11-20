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

package org.apache.gravitino.maintenance.optimizer.recommender.job;

import java.util.Map;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;

/**
 * Translates optimizer job execution context into Gravitino job submission inputs.
 *
 * <p>Usage: {@link GravitinoJobSubmitter} looks up an adapter for the requested job template,
 * creates a new instance, and calls {@link #jobConfig(JobExecutionContext, Map, String)} when
 * submitting the job to Gravitino. The schema separator must come from {@code GET /configs};
 * built-in adapters never invent a default {@code :}.
 */
public interface GravitinoJobAdapter {

  /**
   * Builds job configuration using submitter defaults and the server's {@code
   * gravitino.schema.separator}.
   *
   * @param jobExecutionContext job execution context
   * @param submitterConfigs {@code gravitino.optimizer.jobSubmitterConfig.*} values (may be empty)
   * @param schemaSeparator configured external schema separator from {@code GET /configs}
   * @return job configuration map
   */
  Map<String, String> jobConfig(
      JobExecutionContext jobExecutionContext,
      Map<String, String> submitterConfigs,
      String schemaSeparator);

  /**
   * Same as {@link #jobConfig(JobExecutionContext, Map, String)} with empty submitter defaults.
   *
   * @param jobExecutionContext job execution context
   * @param schemaSeparator configured external schema separator
   * @return job configuration map
   */
  default Map<String, String> jobConfig(
      JobExecutionContext jobExecutionContext, String schemaSeparator) {
    return jobConfig(jobExecutionContext, Map.of(), schemaSeparator);
  }
}
