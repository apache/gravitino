package org.apache.gravitino.job.local;

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
public class LocalJobExecutorConfigs {

  private LocalJobExecutorConfigs() {
    // Private constructor to prevent instantiation
  }

  public static final String LOCAL_JOB_EXECUTOR_NAME = "local";

  public static final String WAITING_QUEUE_SIZE = "waitingQueueSize";
  public static final int DEFAULT_WAITING_QUEUE_SIZE = 100;

  public static final String MAX_RUNNING_JOBS = "maxRunningJobs";
  public static final int DEFAULT_MAX_RUNNING_JOBS =
      Math.max(1, Math.min(Runtime.getRuntime().availableProcessors() / 2, 10));

  public static final String JOB_STATUS_KEEP_TIME_MS = "jobStatusKeepTimeInMs";
  public static final long DEFAULT_JOB_STATUS_KEEP_TIME_MS = 60 * 60 * 1000; // 1 hour

  /**
   * How long in milliseconds a cancelled job's process may keep running after it is asked to stop
   * before the executor kills it forcibly. Must be greater than 0.
   */
  public static final String CANCEL_FORCE_KILL_DELAY_MS = "cancelForceKillDelayInMs";

  /** The default value of {@link #CANCEL_FORCE_KILL_DELAY_MS}. */
  public static final long DEFAULT_CANCEL_FORCE_KILL_DELAY_MS = 30 * 1000L; // 30 seconds

  public static final String SPARK_HOME = "sparkHome";

  /**
   * The job staging directory, set by Gravitino from {@code gravitino.job.stagingDir} rather than
   * by users. A value configured under the local job executor's prefix is overridden.
   */
  public static final String STAGING_DIR = "stagingDir";
}
