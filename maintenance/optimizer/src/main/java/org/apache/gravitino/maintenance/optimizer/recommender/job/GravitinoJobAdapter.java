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
 * creates a new instance, and calls {@link #jobConfig(JobExecutionContext)} when submitting the job
 * to Gravitino.
 */
public interface GravitinoJobAdapter {

  /**
   * Returns the Gravitino job configuration map derived from the execution context.
   *
   * <p>The returned map is supplied as job parameters when submitting the job to Gravitino.
   *
   * @param jobExecutionContext job execution context
   * @return job configuration map
   */
  Map<String, String> jobConfig(JobExecutionContext jobExecutionContext);
}
