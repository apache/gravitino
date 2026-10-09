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

package org.apache.gravitino.connector.job;

import com.google.common.base.Preconditions;
import java.io.File;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.job.JobTemplate;

/**
 * The context of a job run, which Gravitino passes to the job executor together with the runtime
 * job template when it submits the job.
 */
@DeveloperApi
public final class JobContext {

  private final long jobId;

  private final String metalake;

  private final File stagingDir;

  /**
   * Creates the context of a job run.
   *
   * @param jobId the Gravitino job id
   * @param metalake the metalake the job belongs to
   * @param stagingDir the staging directory of the job
   */
  public JobContext(long jobId, String metalake, File stagingDir) {
    Preconditions.checkArgument(StringUtils.isNotBlank(metalake), "metalake must not be blank");
    Preconditions.checkArgument(stagingDir != null, "stagingDir must not be null");
    this.jobId = jobId;
    this.metalake = metalake;
    this.stagingDir = stagingDir;
  }

  /**
   * Returns the Gravitino job id, which is unique across all metalakes. It is not the job execution
   * id that the job executor returns from {@link JobExecutor#submitJob(JobContext, JobTemplate)}.
   *
   * @return the Gravitino job id
   */
  public long jobId() {
    return jobId;
  }

  /**
   * Returns the metalake the job belongs to.
   *
   * @return the metalake name
   */
  public String metalake() {
    return metalake;
  }

  /**
   * Returns a directory on the Gravitino server dedicated to this job, where a job executor can
   * localize the job's resources, for example with {@link JobResourceUtils#localizeJobTemplate}.
   * Gravitino creates it before submitting the job, removes it right away if the job fails to be
   * submitted, and otherwise cleans it up some time after the job finishes ({@code
   * gravitino.job.stagingDirKeepTimeInMs}).
   *
   * @return the staging directory of the job
   */
  public File stagingDir() {
    return stagingDir;
  }

  @Override
  public String toString() {
    return "JobContext{jobId="
        + jobId
        + ", metalake="
        + metalake
        + ", stagingDir="
        + stagingDir
        + "}";
  }
}
