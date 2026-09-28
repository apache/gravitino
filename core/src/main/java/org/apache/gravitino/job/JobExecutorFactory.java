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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.connector.job.JobContext;
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
        Maps.newHashMap(
            config.getConfigsWithPrefix(JOB_EXECUTOR_CONF_PREFIX + jobExecutorName + "."));
    try {
      Class<?> jobExecutorClass = Class.forName(clzName);
      checkJobExecutorClass(jobExecutorClass);
      JobExecutor jobExecutor =
          (JobExecutor) jobExecutorClass.getDeclaredConstructor().newInstance();
      if (jobExecutor instanceof LocalJobExecutor) {
        // The local job executor, and any subclass of it, keeps its output index under the job
        // staging directory, so it must resolve paths against exactly the directory JobManager
        // stages jobs in.
        configs.put(LocalJobExecutorConfigs.STAGING_DIR, config.get(Configs.JOB_STAGING_DIR));
      }
      jobExecutor.initialize(configs);
      return jobExecutor;

    } catch (Exception e) {
      throw new RuntimeException("Failed to create job executor: " + jobExecutorName, e);
    }
  }

  /**
   * Checks that the job executor class implements all the methods Gravitino requires. A class
   * compiled against an older version of {@link JobExecutor} still loads, but calling a method it
   * doesn't implement throws {@link AbstractMethodError} later, so it is rejected up front.
   *
   * @param jobExecutorClass The job executor class to check.
   * @throws IllegalArgumentException If the class isn't a job executor, or misses a required
   *     method.
   */
  @VisibleForTesting
  static void checkJobExecutorClass(Class<?> jobExecutorClass) {
    Preconditions.checkArgument(
        JobExecutor.class.isAssignableFrom(jobExecutorClass),
        "%s doesn't implement %s",
        jobExecutorClass.getName(),
        JobExecutor.class.getName());

    Method getJobExecutionInfo;
    try {
      getJobExecutionInfo = jobExecutorClass.getMethod("getJobExecutionInfo", String.class);
    } catch (NoSuchMethodException e) {
      // Never happens for a JobExecutor, as the interface declares the method.
      throw new IllegalArgumentException(e);
    }
    Preconditions.checkArgument(
        !Modifier.isAbstract(getJobExecutionInfo.getModifiers()),
        "Job executor %s doesn't implement JobExecutor#getJobExecutionInfo(String), which "
            + "Gravitino uses to track the jobs. It was likely built against an older version of "
            + "Gravitino, rebuild it against this version and implement the method.",
        jobExecutorClass.getName());

    // Both submit methods have a default implementation, one delegating to the other, so a job
    // executor must implement at least one of them.
    Preconditions.checkArgument(
        implementsMethod(jobExecutorClass, "submitJob", JobContext.class, JobTemplate.class)
            || implementsMethod(jobExecutorClass, "submitJob", JobTemplate.class),
        "Job executor %s implements neither JobExecutor#submitJob(JobContext, JobTemplate) nor "
            + "JobExecutor#submitJob(JobTemplate), implement one of them to submit jobs.",
        jobExecutorClass.getName());

    // LocalJobExecutor submits jobs through submitJob(JobContext, JobTemplate) and never calls the
    // deprecated submitJob(JobTemplate), so a subclass that overrides only the latter would have
    // its override silently skipped.
    Preconditions.checkArgument(
        !LocalJobExecutor.class.isAssignableFrom(jobExecutorClass)
            || !implementsMethod(jobExecutorClass, "submitJob", JobTemplate.class)
            || declaringClassOf(jobExecutorClass, "submitJob", JobContext.class, JobTemplate.class)
                != LocalJobExecutor.class,
        "Job executor %s extends LocalJobExecutor and overrides the deprecated "
            + "JobExecutor#submitJob(JobTemplate), which LocalJobExecutor never calls. Override "
            + "submitJob(JobContext, JobTemplate) instead.",
        jobExecutorClass.getName());
  }

  private static boolean implementsMethod(
      Class<?> jobExecutorClass, String name, Class<?>... parameterTypes) {
    // An inherited default method of the interface is declared by the interface itself.
    return declaringClassOf(jobExecutorClass, name, parameterTypes) != JobExecutor.class;
  }

  private static Class<?> declaringClassOf(
      Class<?> jobExecutorClass, String name, Class<?>... parameterTypes) {
    try {
      return jobExecutorClass.getMethod(name, parameterTypes).getDeclaringClass();
    } catch (NoSuchMethodException e) {
      // Never happens for a JobExecutor, as the interface declares the method.
      throw new IllegalArgumentException(e);
    }
  }
}
