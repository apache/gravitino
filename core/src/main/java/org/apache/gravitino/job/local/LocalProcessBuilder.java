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
package org.apache.gravitino.job.local;

import java.io.File;
import java.util.Map;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.ShellJobTemplate;
import org.apache.gravitino.job.SparkJobTemplate;

public abstract class LocalProcessBuilder {

  /** The name of the file that captures the job process's standard output. */
  public static final String STDOUT_FILE_NAME = "output.log";

  /** The name of the file that captures the job process's standard error. */
  public static final String STDERR_FILE_NAME = "error.log";

  protected final JobTemplate jobTemplate;

  protected final File workingDirectory;

  protected LocalProcessBuilder(
      JobTemplate jobTemplate, File workingDirectory, Map<String, String> configs) {
    this.jobTemplate = jobTemplate;
    this.workingDirectory = workingDirectory;
  }

  public abstract Process start();

  /**
   * Creates the process builder of a job.
   *
   * @param jobTemplate the localized job template of the job
   * @param workingDirectory the working directory of the job process, where the job's resources are
   *     localized and its output is written
   * @param configs the local job executor configurations
   * @return the process builder of the job
   */
  public static LocalProcessBuilder create(
      JobTemplate jobTemplate, File workingDirectory, Map<String, String> configs) {
    if (jobTemplate instanceof ShellJobTemplate) {
      return new ShellProcessBuilder((ShellJobTemplate) jobTemplate, workingDirectory, configs);
    } else if (jobTemplate instanceof SparkJobTemplate) {
      return new SparkProcessBuilder((SparkJobTemplate) jobTemplate, workingDirectory, configs);
    } else {
      throw new IllegalArgumentException(
          "Unsupported job template type: " + jobTemplate.getClass().getName());
    }
  }
}
