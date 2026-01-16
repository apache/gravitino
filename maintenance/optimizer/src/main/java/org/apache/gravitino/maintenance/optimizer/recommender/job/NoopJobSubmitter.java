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

import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Job submitter that logs requests without submitting any jobs. */
public class NoopJobSubmitter implements JobSubmitter {
  private final Logger LOG = LoggerFactory.getLogger(NoopJobSubmitter.class);

  public static final String NAME = "noop-job-submitter";

  /**
   * Logs the job submission request and returns an empty job id.
   *
   * @param jobTemplateName job template name
   * @param jobExecutionContext job execution context
   * @return empty job id
   */
  @Override
  public String submitJob(String jobTemplateName, JobExecutionContext jobExecutionContext) {
    LOG.info(
        "NoopJobSubmitter submitJob: template={}, identifier={}, jobExecuteContext={}",
        jobTemplateName,
        jobExecutionContext.nameIdentifier(),
        jobExecutionContext);
    return "";
  }

  /**
   * Returns the provider name for configuration lookup.
   *
   * @return provider name
   */
  @Override
  public String name() {
    return NAME;
  }

  /**
   * No-op initialization hook.
   *
   * @param optimizerEnv optimizer environment
   */
  @Override
  public void initialize(OptimizerEnv optimizerEnv) {}

  /**
   * No-op close hook.
   *
   * @throws Exception never thrown
   */
  @Override
  public void close() throws Exception {}
}
