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

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;

/**
 * Represents a job template for executing shell scripts. This class extends the JobTemplate class
 * and provides functionality specific to shell job templates, including a list of scripts to be
 * executed.
 *
 * <p>Scripts are a list of shell files that will be leveraged by the "executable". Scripts must be
 * put in the place where Gravitino server can access them. Current Gravitino can support scripts in
 * the local file system, or on the web server (e.g., HTTP, HTTPS, FTP). Distributed file systems
 * like HDFS or S3 will be supported in the future.
 */
public class ShellJobTemplate extends JobTemplate {

  private final List<String> scripts;

  /**
   * Constructs a ShellJobTemplate with the specified builder.
   *
   * @param builder the builder containing the configuration for the shell job template
   */
  protected ShellJobTemplate(Builder builder) {
    super(builder);
    this.scripts = builder.scripts;
  }

  /**
   * Returns the list of scripts to be executed by the shell job template.
   *
   * @return the list of scripts
   */
  public List<String> scripts() {
    return scripts;
  }

  @Override
  public JobType jobType() {
    return JobType.SHELL;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ShellJobTemplate)) return false;
    if (!super.equals(o)) return false;

    ShellJobTemplate that = (ShellJobTemplate) o;
    return Objects.equals(scripts, that.scripts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), scripts);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (!scripts.isEmpty()) {
      sb.append("  scripts=[\n");
      scripts.forEach(s -> sb.append("    ").append(s).append("\n"));
      sb.append("  ]\n");
    } else {
      sb.append("  scripts=[]\n");
    }

    return "\nShellJobTemplate{\n" + super.toString() + sb + "}\n";
  }

  /**
   * Creates a new builder for constructing instances of {@link ShellJobTemplate}.
   *
   * @return a new instance of {@link Builder}
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for creating instances of {@link ShellJobTemplate}. */
  public static class Builder extends BaseBuilder<Builder, ShellJobTemplate> {

    private List<String> scripts;

    private Builder() {}

    /**
     * Sets the scripts to be executed by the shell job template.
     *
     * @param scripts the list of scripts to be executed
     * @return the builder instance for method chaining
     */
    public Builder withScripts(List<String> scripts) {
      this.scripts = scripts;
      return this;
    }

    /**
     * Build the ShellJobTemplate instance.
     *
     * @return the constructed ShellJobTemplate
     */
    @Override
    public ShellJobTemplate build() {
      validate();
      return new ShellJobTemplate(this);
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    protected void validate() {
      super.validate();

      this.scripts = scripts != null ? ImmutableList.copyOf(scripts) : ImmutableList.of();
    }
  }
}
