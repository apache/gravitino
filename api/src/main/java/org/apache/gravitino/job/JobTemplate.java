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
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

/**
 * JobTemplate is a class to define all the configuration parameters for a job.
 *
 * <p>JobType is enum to define the type of the job, because Gravitino needs different runtime
 * environments to execute different types of jobs, so the job type is required.
 *
 * <p>Some parameters can be templated, which means that they can be replaced with actual values
 * when running the job, for example, arguments can be { "{{input_path}}", "{{output_path}}" },
 * environment variables can be { "foo": "{{foo_value}}", "bar": "{{bar_value}}" }. the parameters
 * support templating are:
 *
 * <ul>
 *   <li>arguments
 *   <li>environments
 * </ul>
 *
 * <p>executable is the path to the executable that will be run, it should be an absolute path that
 * can be accessed by the Gravitino server, current Gravitino can support executables in the local
 * file system, or on the web server (e.g., HTTP or HTTPS, FTP). Distributed file systems like HDFS
 * or S3 will be supported in the future.
 */
public abstract class JobTemplate {

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

  /** The name of the job template. */
  protected final String name;

  /** The comment or description of the job template. */
  protected final String comment;

  /** The executable path for the job template. */
  protected final String executable;

  /** The list of arguments for the job template. */
  protected final List<String> arguments;

  /** The map of environment variables for the job template. */
  protected final Map<String, String> environments;

  /** The map of custom fields for the job template. */
  protected final Map<String, String> customFields;

  /**
   * Constructs a JobTemplate instance with the specified parameters.
   *
   * @param builder the builder containing the job template parameters
   */
  protected JobTemplate(BaseBuilder<?, ?> builder) {
    this.name = builder.name;
    this.comment = builder.comment;
    this.executable = builder.executable;
    this.arguments = builder.arguments;
    this.environments = builder.environments;
    this.customFields = builder.customFields;
  }

  /**
   * Get the job type.
   *
   * @return the type of the job template
   */
  public abstract JobType jobType();

  /**
   * Get the name of the job template.
   *
   * @return the name of the job template
   */
  public String name() {
    return name;
  }

  /**
   * Get the comment for the job template.
   *
   * @return the comment
   */
  public String comment() {
    return comment;
  }

  /**
   * Get the executable for the job template.
   *
   * @return the executable path
   */
  public String executable() {
    return executable;
  }

  /**
   * Get the arguments for the job template.
   *
   * @return the list of arguments
   */
  public List<String> arguments() {
    return arguments;
  }

  /**
   * Get the environment variables for the job template.
   *
   * @return the map of environment variables
   */
  public Map<String, String> environments() {
    return environments;
  }

  /**
   * Get the custom fields for the job template.
   *
   * @return the map of custom fields
   */
  public Map<String, String> customFields() {
    return customFields;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JobTemplate)) {
      return false;
    }

    JobTemplate that = (JobTemplate) o;
    return Objects.equals(jobType(), that.jobType())
        && Objects.equals(name, that.name)
        && Objects.equals(comment, that.comment)
        && Objects.equals(executable, that.executable)
        && Objects.equals(arguments, that.arguments)
        && Objects.equals(environments, that.environments)
        && Objects.equals(customFields, that.customFields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        jobType(), name, comment, executable, arguments, environments, customFields);
  }

  @Override
  public String toString() {
    StringBuilder sb =
        new StringBuilder("  jobType=")
            .append(jobType())
            .append(",\n")
            .append("  name='")
            .append(name)
            .append("',\n");
    if (StringUtils.isNotBlank(comment)) {
      sb.append("  comment='").append(comment).append("',\n");
    }

    sb.append("  executable='").append(executable).append("',\n");

    if (!arguments.isEmpty()) {
      sb.append("  arguments=[\n");
      arguments.forEach(arg -> sb.append("    ").append(arg).append(",\n"));
      sb.append("  ],\n");
    } else {
      sb.append("  arguments=[],\n");
    }

    if (!environments.isEmpty()) {
      sb.append("  environments={\n");
      environments.forEach(
          (k, v) -> sb.append("    ").append(k).append(": ").append(v).append(",\n"));
      sb.append("  },\n");
    } else {
      sb.append("  environments={},\n");
    }

    if (!customFields.isEmpty()) {
      sb.append("  customFields={\n");
      customFields.forEach(
          (k, v) -> sb.append("    ").append(k).append(": ").append(v).append(",\n"));
      sb.append("  }\n");
    } else {
      sb.append("  customFields={}\n");
    }

    return sb.toString();
  }

  /** Builder class for constructing JobTemplate instances. */
  public abstract static class BaseBuilder<B extends BaseBuilder<B, P>, P extends JobTemplate> {

    private String name;
    private String comment;
    private String executable;
    private List<String> arguments;
    private Map<String, String> environments;
    private Map<String, String> customFields;

    /**
     * Constructor for the BaseBuilder. This constructor is protected to ensure that only subclasses
     * can instantiate it.
     */
    protected BaseBuilder() {}

    /**
     * Returns the current instance of the builder.
     *
     * @return the current builder instance
     */
    protected abstract B self();

    /**
     * Build the JobTemplate instance.
     *
     * @return the constructed JobTemplate instance
     */
    public abstract P build();

    /**
     * Set the name of the job template.
     *
     * @param name the name of the job template.
     * @return this Builder instance
     */
    public B withName(String name) {
      this.name = name;
      return self();
    }

    /**
     * Set the comment for the job template.
     *
     * @param comment the comment or description of the job template
     * @return this Builder instance
     */
    public B withComment(String comment) {
      this.comment = comment;
      return self();
    }

    /**
     * Set the executable for the job template.
     *
     * @param executable the path to the executable
     * @return this Builder instance
     */
    public B withExecutable(String executable) {
      this.executable = executable;
      return self();
    }

    /**
     * Set the arguments for the job template.
     *
     * @param arguments the list of arguments
     * @return this Builder instance
     */
    public B withArguments(List<String> arguments) {
      this.arguments = arguments;
      return self();
    }

    /**
     * Set the environment variables for the job template.
     *
     * @param environments the map of environment variables
     * @return this Builder instance
     */
    public B withEnvironments(Map<String, String> environments) {
      this.environments = environments;
      return self();
    }

    /**
     * Set the custom fields for the job template.
     *
     * @param customFields the map of custom fields
     * @return this Builder instance
     */
    public B withCustomFields(Map<String, String> customFields) {
      this.customFields = customFields;
      return self();
    }

    /** Validates the parameters of the job template. */
    protected void validate() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(name), "Job name must not be null or empty");

      Preconditions.checkArgument(
          StringUtils.isNotBlank(executable), "Executable must not be null or empty");

      this.arguments = arguments != null ? ImmutableList.copyOf(arguments) : ImmutableList.of();
      this.environments =
          environments != null ? ImmutableMap.copyOf(environments) : ImmutableMap.of();
      this.customFields =
          customFields != null ? ImmutableMap.copyOf(customFields) : ImmutableMap.of();
    }
  }
}
