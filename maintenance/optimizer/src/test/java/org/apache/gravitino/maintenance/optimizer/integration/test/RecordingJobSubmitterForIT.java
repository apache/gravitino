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

package org.apache.gravitino.maintenance.optimizer.integration.test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;

public class RecordingJobSubmitterForIT implements JobSubmitter {
  public static final String NAME = "recording-job-submitter-it";
  public static final String SESSION_ID_KEY =
      OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "recording-session-id";

  private static final Map<String, List<JobExecutionContext>> SUBMITTED_CONTEXTS_BY_SESSION =
      new ConcurrentHashMap<>();

  private String sessionId;

  public static void reset(String sessionId) {
    SUBMITTED_CONTEXTS_BY_SESSION.put(sessionId, new CopyOnWriteArrayList<>());
  }

  public static List<JobExecutionContext> submittedContexts(String sessionId) {
    return List.copyOf(
        SUBMITTED_CONTEXTS_BY_SESSION.getOrDefault(sessionId, new CopyOnWriteArrayList<>()));
  }

  public static void clear(String sessionId) {
    SUBMITTED_CONTEXTS_BY_SESSION.remove(sessionId);
  }

  @Override
  public String submitJob(String jobTemplateName, JobExecutionContext jobExecutionContext) {
    List<JobExecutionContext> submittedContexts =
        SUBMITTED_CONTEXTS_BY_SESSION.computeIfAbsent(
            sessionId, key -> new CopyOnWriteArrayList<>());
    submittedContexts.add(jobExecutionContext);
    return "it-job-" + submittedContexts.size();
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    this.sessionId = optimizerEnv.config().getAllConfig().get(SESSION_ID_KEY);
    if (sessionId == null || sessionId.isEmpty()) {
      throw new IllegalArgumentException(
          "Missing test session id config for RecordingJobSubmitterForIT: " + SESSION_ID_KEY);
    }
  }

  @Override
  public void close() throws Exception {}
}
