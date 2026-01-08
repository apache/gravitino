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

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.util.GravitinoClientUtils;

/** Submits optimizer jobs to Gravitino using job template adapters. */
public class GravitinoJobSubmitter implements JobSubmitter {

  public static final String NAME = "gravitino-job-submitter";

  private GravitinoClient gravitinoClient;

  private final Map<String, Class<? extends GravitinoJobAdapter>> jobAdapters = Map.of();

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
   * Initializes the submitter with a Gravitino client derived from the optimizer configuration.
   *
   * @param optimizerEnv optimizer environment
   */
  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    this.gravitinoClient = GravitinoClientUtils.createClient(optimizerEnv);
  }

  /**
   * Submits a job through Gravitino using the resolved job adapter.
   *
   * @param jobTemplateName template name used to select an adapter
   * @param jobExecutionContext execution context for the job
   * @return submitted job identifier
   */
  @Override
  public String submitJob(String jobTemplateName, JobExecutionContext jobExecutionContext) {
    GravitinoJobAdapter jobAdapter = loadJobAdapter(jobTemplateName);
    return gravitinoClient
        .runJob(jobTemplateName, jobAdapter.jobConfig(jobExecutionContext))
        .jobId();
  }

  /** Closes the underlying Gravitino client. */
  @Override
  public void close() throws Exception {
    if (gravitinoClient != null) {
      gravitinoClient.close();
    }
  }

  @VisibleForTesting
  GravitinoJobAdapter loadJobAdapter(String jobTemplateName) {
    Class<? extends GravitinoJobAdapter> jobAdapterClz = jobAdapters.get(jobTemplateName);
    if (jobAdapterClz == null) {
      throw new IllegalArgumentException("No job adapter found for template: " + jobTemplateName);
    }
    try {
      return jobAdapterClz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to create job adapter for template: " + jobTemplateName, e);
    }
  }
}
