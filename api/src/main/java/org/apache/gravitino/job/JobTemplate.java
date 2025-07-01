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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * JobTemplate is a class to define all the configuration parameters for a job.
 *
 * <p>JobType is enum to define the type of the job, because Gravitino needs different runtime
 * environments to execute different types of jobs, so the job type is required.
 *
 * <p>Some parameters can be templated, which means that they can be replaced with actual values
 * when running the job, for example, arguments can be { "{input_path}}", "{{output_path}" },
 * environment variables can be { "foo": "{{foo_value}}", "bar": "{{bar_value}}" }. the parameters
 * support templating are:
 *
 * <ul>
 *   <li>arguments
 *   <li>environments
 *   <li>configs
 * </ul>
 *
 * The artifacts (files, jars, archives) will be uploaded to the Gravitino server and managed in the
 * staging path when registering the job.
 */
public final class JobTemplate {

  /**
   * JobType is an enum to define the type of the job.
   *
   * <p>Gravitino supports different types of jobs, such as Spark and Shell. The job type is
   * required to determine the runtime environment for executing the job.
   */
  public enum JobType {

    /** Job type for executing a Spark application. */
    SPARK,

    /** Job type for executing a shell command. */
    SHELL,
  }

  private JobType jobType;

  private String name;

  private String comment;

  private String executable;

  private List<String> arguments;

  private Map<String, String> configs;

  private Map<String, String> environments;

  private List<String> files;

  private List<String> jars;

  private List<String> archives;

  private JobTemplate() {
    // Prevent instantiation
  }

  /**
   * Get the job type.
   *
   * @return the type of the job
   */
  public JobType jobType() {
    return jobType;
  }

  /**
   * Get the name of the job.
   *
   * @return the name of the job
   */
  public String name() {
    return name;
  }

  /**
   * Get the comment for the job.
   *
   * @return the comment
   */
  public String comment() {
    return comment;
  }

  /**
   * Get the executable for the job.
   *
   * @return the executable path
   */
  public String executable() {
    return executable;
  }

  /**
   * Get the arguments for the job.
   *
   * @return the list of arguments
   */
  public List<String> arguments() {
    return arguments;
  }

  /**
   * Get the configurations for the job.
   *
   * @return the map of configurations
   */
  public Map<String, String> configs() {
    return configs;
  }

  /**
   * Get the environment variables for the job.
   *
   * @return the map of environment variables
   */
  public Map<String, String> environments() {
    return environments;
  }

  /**
   * Get the files to be included in the job.
   *
   * @return the list of file paths
   */
  public List<String> files() {
    return files;
  }

  /**
   * Get the jars to be included in the job.
   *
   * @return the list of jar paths
   */
  public List<String> jars() {
    return jars;
  }

  /**
   * Get the archives to be included in the job.
   *
   * @return the list of archive paths
   */
  public List<String> archives() {
    return archives;
  }

  /**
   * Create a new Builder instance for JobTemplate.
   *
   * @return a new Builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder class for constructing JobTemplate instances. */
  public static class Builder {
    private final JobTemplate jobTemplate;

    private Builder() {
      this.jobTemplate = new JobTemplate();
    }

    /**
     * Set the job type.
     *
     * @param jobType the type of the job
     * @return this Builder instance
     */
    public Builder withJobType(JobType jobType) {
      jobTemplate.jobType = jobType;
      return this;
    }

    /**
     * Set the name of the job.
     *
     * @param name the name of the job
     * @return this Builder instance
     */
    public Builder withName(String name) {
      jobTemplate.name = name;
      return this;
    }

    /**
     * Set the comment for the job.
     *
     * @param comment the comment or description of the job
     * @return this Builder instance
     */
    public Builder withComment(String comment) {
      jobTemplate.comment = comment;
      return this;
    }

    /**
     * Set the executable for the job.
     *
     * @param executable the path to the executable
     * @return this Builder instance
     */
    public Builder withExecutable(String executable) {
      jobTemplate.executable = executable;
      return this;
    }

    /**
     * Set the arguments for the job.
     *
     * @param arguments the list of arguments
     * @return this Builder instance
     */
    public Builder withArguments(List<String> arguments) {
      jobTemplate.arguments = arguments;
      return this;
    }

    /**
     * Set the configurations for the job.
     *
     * @param configs the map of configurations
     * @return this Builder instance
     */
    public Builder withConfigs(Map<String, String> configs) {
      jobTemplate.configs = configs;
      return this;
    }

    /**
     * Set the environment variables for the job.
     *
     * @param environments the map of environment variables
     * @return this Builder instance
     */
    public Builder withEnvironments(Map<String, String> environments) {
      jobTemplate.environments = environments;
      return this;
    }

    /**
     * Set the files to be included in the job.
     *
     * @param files the list of file paths
     * @return this Builder instance
     */
    public Builder withFiles(List<String> files) {
      jobTemplate.files = files;
      return this;
    }

    /**
     * Set the jars to be included in the job.
     *
     * @param jars the list of jar paths
     * @return this Builder instance
     */
    public Builder withJars(List<String> jars) {
      jobTemplate.jars = jars;
      return this;
    }

    /**
     * Set the archives to be included in the job.
     *
     * @param archives the list of archive paths
     * @return this Builder instance
     */
    public Builder withArchives(List<String> archives) {
      jobTemplate.archives = archives;
      return this;
    }

    /**
     * Build the jobTemplate instance.
     *
     * @return a new jobTemplate instance
     * @throws IllegalArgumentException if the executable is null or empty
     */
    public JobTemplate build() {
      Preconditions.checkArgument(jobTemplate.jobType != null, "Job type must not be null");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(jobTemplate.name), "Job name must not be null or empty");

      Preconditions.checkArgument(
          StringUtils.isNotBlank(jobTemplate.executable), "Executable must not be null or empty");

      jobTemplate.arguments =
          jobTemplate.arguments != null
              ? ImmutableList.copyOf(jobTemplate.arguments)
              : ImmutableList.of();
      jobTemplate.configs =
          jobTemplate.configs != null
              ? ImmutableMap.copyOf(jobTemplate.configs)
              : ImmutableMap.of();
      jobTemplate.environments =
          jobTemplate.environments != null
              ? ImmutableMap.copyOf(jobTemplate.environments)
              : ImmutableMap.of();
      jobTemplate.files =
          jobTemplate.files != null ? ImmutableList.copyOf(jobTemplate.files) : ImmutableList.of();
      jobTemplate.jars =
          jobTemplate.jars != null ? ImmutableList.copyOf(jobTemplate.jars) : ImmutableList.of();
      jobTemplate.archives =
          jobTemplate.archives != null
              ? ImmutableList.copyOf(jobTemplate.archives)
              : ImmutableList.of();

      return jobTemplate;
    }
  }
}
