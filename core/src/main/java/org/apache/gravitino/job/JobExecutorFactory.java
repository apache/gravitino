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
package org.apache.gravitino.job;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.connector.job.JobExecutor;
import org.apache.gravitino.job.local.LocalJobExecutor;
import org.apache.gravitino.job.local.LocalJobExecutorConfigs;

public class JobExecutorFactory {

  private static final String JOB_EXECUTOR_CONF_PREFIX = "gravitino.jobExecutor.";

  private static final String JOB_EXECUTOR_CLASS_SUFFIX = ".class";

  private static final Map<String, String> BUILTIN_EXECUTORS =
      ImmutableMap.of(
          LocalJobExecutorConfigs.LOCAL_JOB_EXECUTOR_NAME,
          LocalJobExecutor.class.getCanonicalName());

  private JobExecutorFactory() {
    // Private constructor to prevent instantiation
  }

  public static JobExecutor create(Config config) {
    String jobExecutorName = config.get(Configs.JOB_EXECUTOR);
    String clzName;
    if (BUILTIN_EXECUTORS.containsKey(jobExecutorName)) {
      clzName = BUILTIN_EXECUTORS.get(jobExecutorName);
    } else {
      String jobExecutorClassKey =
          JOB_EXECUTOR_CONF_PREFIX + jobExecutorName + JOB_EXECUTOR_CLASS_SUFFIX;
      clzName = config.getRawString(jobExecutorClassKey);
    }

    Preconditions.checkArgument(
        StringUtils.isNotBlank(clzName),
        "Job executor class name must be specified for job executor: %s",
        jobExecutorName);

    Map<String, String> configs =
        config.getConfigsWithPrefix(JOB_EXECUTOR_CONF_PREFIX + jobExecutorName + ".");
    try {
      JobExecutor jobExecutor =
          (JobExecutor) Class.forName(clzName).getDeclaredConstructor().newInstance();
      jobExecutor.initialize(configs);
      return jobExecutor;

    } catch (Exception e) {
      throw new RuntimeException("Failed to create job executor: " + jobExecutorName, e);
    }
  }
}
