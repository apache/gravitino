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
 * Represents a job template for executing Spark applications. This class extends the JobTemplate
 * class and provides functionality specific to Spark job templates, including the class name, jars,
 * files, archives, and configurations required for the Spark job.
 *
 * <p>Take Spark word count job as an example:
 *
 * <p>className: "org.apache.spark.examples.JavaWordCount" executable
 * "https://example.com/spark-examples.jar" arguments: ["{{input_path}}", "{{output_path}}"]
 * configs: {"spark.master": "local[*]", "spark.app.name": "WordCount"}
 *
 * <p>configs is a map of configuration parameters that will be used by the Spark application. It
 * can be templated by using placeholders like "{{foo_value}}" and "{{bar_value}}". These
 * placeholders will be replaced with actual values when the job is executed.
 *
 * <p>jars, files, and archives are lists of resources that will be used by the Spark application .
 * These resources must be accessible to the Gravitino server, and can be located in the local file
 * system, on a web server (e.g., HTTP, HTTPS, FTP). Distributed file systems like HDFS or S3 will
 * be supported in the future.
 */
public class SparkJobTemplate extends JobTemplate {

  private final String className;

  private final List<String> jars;

  private final List<String> files;

  private final List<String> archives;

  private final Map<String, String> configs;

  /**
   * Constructs a SparkJobTemplate with the specified builder.
   *
   * @param builder the builder containing the configuration for the Spark job template
   */
  protected SparkJobTemplate(Builder builder) {
    super(builder);
    this.className = builder.className;
    this.jars = builder.jars;
    this.files = builder.files;
    this.archives = builder.archives;
    this.configs = builder.configs;
  }

  /**
   * Returns the class name of the Spark application to be executed.
   *
   * @return the class name
   */
  public String className() {
    return className;
  }

  /**
   * Returns the list of JAR files required for the Spark job.
   *
   * @return the list of JAR files
   */
  public List<String> jars() {
    return jars;
  }

  /**
   * Returns the list of files required for the Spark job.
   *
   * @return the list of files
   */
  public List<String> files() {
    return files;
  }

  /**
   * Returns the list of archives required for the Spark job.
   *
   * @return the list of archives
   */
  public List<String> archives() {
    return archives;
  }

  /**
   * Returns the configuration map for the Spark job.
   *
   * @return the configuration map
   */
  public Map<String, String> configs() {
    return configs;
  }

  @Override
  public JobType jobType() {
    return JobType.SPARK;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SparkJobTemplate)) return false;
    if (!super.equals(o)) return false;

    SparkJobTemplate that = (SparkJobTemplate) o;
    return Objects.equals(className, that.className)
        && Objects.equals(jars, that.jars)
        && Objects.equals(files, that.files)
        && Objects.equals(archives, that.archives)
        && Objects.equals(configs, that.configs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), className, jars, files, archives, configs);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("\nSparkJobTemplate{\n");
    sb.append("  className='").append(className).append("',\n");

    if (!jars.isEmpty()) {
      sb.append("  jars=[\n");
      jars.forEach(j -> sb.append("    ").append(j).append("\n"));
      sb.append("  ],\n");
    } else {
      sb.append("  jars=[],\n");
    }

    if (!files.isEmpty()) {
      sb.append("  files=[\n");
      files.forEach(f -> sb.append("    ").append(f).append("\n"));
      sb.append("  ],\n");
    } else {
      sb.append("  files=[],\n");
    }

    if (!archives.isEmpty()) {
      sb.append("  archives=[\n");
      archives.forEach(a -> sb.append("    ").append(a).append("\n"));
      sb.append("  ],\n");
    } else {
      sb.append("  archives=[],\n");
    }

    if (!configs.isEmpty()) {
      sb.append("  configs={\n");
      configs.forEach((k, v) -> sb.append("    ").append(k).append(": ").append(v).append(",\n"));
      sb.append("  }\n");
    } else {
      sb.append("  configs={}\n");
    }

    return sb + super.toString() + "}\n";
  }

  /**
   * Creates a new builder for constructing instances of {@link SparkJobTemplate}.
   *
   * @return a new instance of {@link Builder}
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for creating instances of {@link SparkJobTemplate}. */
  public static class Builder extends JobTemplate.BaseBuilder<Builder, SparkJobTemplate> {

    private String className;

    private List<String> jars;

    private List<String> files;

    private List<String> archives;

    private Map<String, String> configs;

    private Builder() {}

    /**
     * Sets the class name of the Spark application to be executed.
     *
     * @param className the class name
     * @return the builder instance for method chaining
     */
    public Builder withClassName(String className) {
      this.className = className;
      return this;
    }

    /**
     * Sets the list of JAR files required for the Spark job.
     *
     * @param jars the list of JAR files
     * @return the builder instance for method chaining
     */
    public Builder withJars(List<String> jars) {
      this.jars = jars;
      return this;
    }

    /**
     * Sets the list of files required for the Spark job.
     *
     * @param files the list of files
     * @return the builder instance for method chaining
     */
    public Builder withFiles(List<String> files) {
      this.files = files;
      return this;
    }

    /**
     * Sets the list of archives required for the Spark job.
     *
     * @param archives the list of archives
     * @return the builder instance for method chaining
     */
    public Builder withArchives(List<String> archives) {
      this.archives = archives;
      return this;
    }

    /**
     * Sets the configuration map for the Spark job.
     *
     * @param configs the configuration map
     * @return the builder instance for method chaining
     */
    public Builder withConfigs(Map<String, String> configs) {
      this.configs = configs;
      return this;
    }

    /**
     * Builds the SparkJobTemplate instance with the specified parameters.
     *
     * @return a new instance of SparkJobTemplate
     */
    @Override
    public SparkJobTemplate build() {
      validate();
      return new SparkJobTemplate(this);
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    protected void validate() {
      super.validate();

      Preconditions.checkArgument(
          StringUtils.isNotBlank(className), "Class name must not be null or empty");

      this.jars = jars != null ? ImmutableList.copyOf(jars) : ImmutableList.of();
      this.files = files != null ? ImmutableList.copyOf(files) : ImmutableList.of();
      this.archives = archives != null ? ImmutableList.copyOf(archives) : ImmutableList.of();
      this.configs = configs != null ? ImmutableMap.copyOf(configs) : ImmutableMap.of();
    }
  }
}
