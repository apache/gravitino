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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

/**
 * The interface for job template changes. A job template change is an operation that modifies a job
 * template. It can be one of the following:
 *
 * <ul>
 *   <li>Rename the job template.
 *   <li>Update the comment of the job template.
 *   <li>Update the job template details, such as executable, arguments, environments, custom
 *       fields, etc.
 * </ul>
 */
public interface JobTemplateChange {

  /**
   * Creates a new job template change to update the name of the job template.
   *
   * @param newName The new name of the job template.
   * @return The job template change.
   */
  static JobTemplateChange rename(String newName) {
    return new RenameJobTemplate(newName);
  }

  /**
   * Creates a new job template change to update the comment of the job template.
   *
   * @param newComment The new comment of the job template.
   * @return The job template change.
   */
  static JobTemplateChange updateComment(String newComment) {
    return new UpdateJobTemplateComment(newComment);
  }

  /**
   * Creates a new job template change to update the details of the job template.
   *
   * @param templateUpdate The job template update details.
   * @return The job template change.
   */
  static JobTemplateChange updateTemplate(TemplateUpdate templateUpdate) {
    return new UpdateJobTemplate(templateUpdate);
  }

  /** A job template change to rename the job template. */
  final class RenameJobTemplate implements JobTemplateChange {
    private final String newName;

    private RenameJobTemplate(String newName) {
      this.newName = newName;
    }

    /**
     * Get the new name of the job template.
     *
     * @return The new name of the job template.
     */
    public String getNewName() {
      return newName;
    }

    /**
     * Checks if this RenameJobTemplate is equal to another object.
     *
     * @param o The object to compare with.
     * @return true if the objects are equal, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      RenameJobTemplate that = (RenameJobTemplate) o;
      return Objects.equals(newName, that.newName);
    }

    /**
     * Generates a hash code for this RenameJobTemplate.
     *
     * @return The hash code.
     */
    @Override
    public int hashCode() {
      return Objects.hash(newName);
    }

    /**
     * Get the string representation of the job template change.
     *
     * @return The string representation of the job template change.
     */
    @Override
    public String toString() {
      return "RENAME JOB TEMPLATE " + newName;
    }
  }

  /** A job template change to update the comment of the job template. */
  final class UpdateJobTemplateComment implements JobTemplateChange {
    private final String newComment;

    private UpdateJobTemplateComment(String newComment) {
      this.newComment = newComment;
    }

    /**
     * Get the new comment of the job template.
     *
     * @return The new comment of the job template.
     */
    public String getNewComment() {
      return newComment;
    }

    /**
     * Checks if this UpdateJobTemplateComment is equal to another object.
     *
     * @param o The object to compare with.
     * @return true if the objects are equal, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      UpdateJobTemplateComment that = (UpdateJobTemplateComment) o;
      return Objects.equals(newComment, that.newComment);
    }

    /**
     * Generates a hash code for this UpdateJobTemplateComment.
     *
     * @return The hash code.
     */
    @Override
    public int hashCode() {
      return Objects.hashCode(newComment);
    }

    /**
     * Get the string representation of the job template change.
     *
     * @return The string representation of the job template change.
     */
    @Override
    public String toString() {
      return "UPDATE JOB TEMPLATE COMMENT " + newComment;
    }
  }

  /** A job template change to update the details of the job template. */
  final class UpdateJobTemplate implements JobTemplateChange {

    private final TemplateUpdate templateUpdate;

    private UpdateJobTemplate(TemplateUpdate templateUpdate) {
      this.templateUpdate = templateUpdate;
    }

    /**
     * Get the template update.
     *
     * @return The template update.
     */
    public TemplateUpdate getTemplateUpdate() {
      return templateUpdate;
    }

    /**
     * Checks if this UpdateTemplate is equal to another object.
     *
     * @param o The object to compare with.
     * @return true if the objects are equal, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      UpdateJobTemplate that = (UpdateJobTemplate) o;
      return Objects.equals(templateUpdate, that.templateUpdate);
    }

    /**
     * Generates a hash code for this UpdateJobTemplate.
     *
     * @return The hash code.
     */
    @Override
    public int hashCode() {
      return Objects.hashCode(templateUpdate);
    }

    /**
     * Get the string representation of the job template change.
     *
     * @return The string representation of the job template change.
     */
    @Override
    public String toString() {
      return "UPDATE JOB TEMPLATE " + templateUpdate.getClass().getSimpleName();
    }
  }

  /** Base class for template updates. */
  abstract class TemplateUpdate {

    private final String newExecutable;

    private final List<String> newArguments;

    private final Map<String, String> newEnvironments;

    private final Map<String, String> newCustomFields;

    /**
     * Constructor for TemplateUpdate.
     *
     * @param builder The builder to construct the TemplateUpdate.
     */
    protected TemplateUpdate(BaseBuilder<?, ?> builder) {
      this.newExecutable = builder.newExecutable;
      this.newArguments = builder.newArguments;
      this.newEnvironments = builder.newEnvironments;
      this.newCustomFields = builder.newCustomFields;
    }

    /**
     * Get the new executable of the job template.
     *
     * @return The new executable of the job template.
     */
    public String getNewExecutable() {
      return newExecutable;
    }

    /**
     * Get the new arguments of the job template.
     *
     * @return The new arguments of the job template.
     */
    public List<String> getNewArguments() {
      return newArguments;
    }

    /**
     * Get the new environments of the job template.
     *
     * @return The new environments of the job template.
     */
    public Map<String, String> getNewEnvironments() {
      return newEnvironments;
    }

    /**
     * Get the new custom fields of the job template.
     *
     * @return The new custom fields of the job template.
     */
    public Map<String, String> getNewCustomFields() {
      return newCustomFields;
    }

    /**
     * Checks if this TemplateUpdate is equal to another object.
     *
     * @param o The object to compare with.
     * @return true if the objects are equal, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (!(o instanceof TemplateUpdate)) return false;

      TemplateUpdate that = (TemplateUpdate) o;
      return Objects.equals(newExecutable, that.newExecutable)
          && Objects.equals(newArguments, that.newArguments)
          && Objects.equals(newEnvironments, that.newEnvironments)
          && Objects.equals(newCustomFields, that.newCustomFields);
    }

    /**
     * Generates a hash code for this TemplateUpdate.
     *
     * @return The hash code.
     */
    @Override
    public int hashCode() {
      return Objects.hash(newExecutable, newArguments, newEnvironments, newCustomFields);
    }

    /** Base builder class for constructing TemplateUpdate instances. */
    protected abstract static class BaseBuilder<
        B extends BaseBuilder<B, P>, P extends TemplateUpdate> {
      private String newExecutable;
      private List<String> newArguments;
      private Map<String, String> newEnvironments;
      private Map<String, String> newCustomFields;

      /** Protected constructor to prevent direct instantiation. */
      protected BaseBuilder() {}

      /**
       * Returns the builder instance itself for method chaining.
       *
       * @return The builder instance.
       */
      protected abstract B self();

      /**
       * Builds the TemplateUpdate instance.
       *
       * @return A new TemplateUpdate instance.
       */
      public abstract P build();

      /**
       * Sets the new executable for the job template.
       *
       * @param newExecutable The new executable to set.
       * @return The builder instance for chaining.
       */
      public B withNewExecutable(String newExecutable) {
        this.newExecutable = newExecutable;
        return self();
      }

      /**
       * Sets the new arguments for the job template.
       *
       * @param newArguments The new arguments to set.
       * @return The builder instance for chaining.
       */
      public B withNewArguments(List<String> newArguments) {
        this.newArguments = newArguments;
        return self();
      }

      /**
       * Sets the new environments for the job template.
       *
       * @param newEnvironments The new environments to set.
       * @return The builder instance for chaining.
       */
      public B withNewEnvironments(Map<String, String> newEnvironments) {
        this.newEnvironments = newEnvironments;
        return self();
      }

      /**
       * Sets the new custom fields for the job template.
       *
       * @param newCustomFields The new custom fields to set.
       * @return The builder instance for chaining.
       */
      public B withNewCustomFields(Map<String, String> newCustomFields) {
        this.newCustomFields = newCustomFields;
        return self();
      }

      /** Validates the builder fields before building the TemplateUpdate instance. */
      protected void validate() {
        Preconditions.checkArgument(
            StringUtils.isNoneBlank(newExecutable), "Executable cannot be null or blank");
        this.newArguments =
            newArguments == null ? Collections.emptyList() : ImmutableList.copyOf(newArguments);
        this.newEnvironments =
            newEnvironments == null ? Collections.emptyMap() : ImmutableMap.copyOf(newEnvironments);
        this.newCustomFields =
            newCustomFields == null ? Collections.emptyMap() : ImmutableMap.copyOf(newCustomFields);
      }
    }
  }

  /** A job template update for shell templates. */
  final class ShellTemplateUpdate extends TemplateUpdate {

    private final List<String> newScripts;

    private ShellTemplateUpdate(Builder builder) {
      super(builder);
      this.newScripts = builder.newScripts;
    }

    /**
     * Get the new scripts of the shell job template.
     *
     * @return The new scripts of the shell job template.
     */
    public List<String> getNewScripts() {
      return newScripts;
    }

    /**
     * Checks if this ShellTemplateUpdate is equal to another object.
     *
     * @param o The object to compare with.
     * @return true if the objects are equal, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      ShellTemplateUpdate that = (ShellTemplateUpdate) o;
      return Objects.equals(newScripts, that.newScripts);
    }

    /**
     * Generates a hash code for this ShellTemplateUpdate.
     *
     * @return The hash code.
     */
    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), newScripts);
    }

    /**
     * Creates a new builder for ShellTemplateUpdate.
     *
     * @return A new Builder instance.
     */
    public static Builder builder() {
      return new Builder();
    }

    /** Builder class for constructing ShellTemplateUpdate instances. */
    public static class Builder extends BaseBuilder<Builder, ShellTemplateUpdate> {

      private List<String> newScripts;

      private Builder() {}

      /**
       * Sets the new scripts for the shell job template.
       *
       * @param newScripts The new scripts to set.
       * @return The builder instance for chaining.
       */
      public Builder withNewScripts(List<String> newScripts) {
        this.newScripts = newScripts;
        return this;
      }

      /**
       * Builds the ShellTemplateUpdate instance.
       *
       * @return A new ShellTemplateUpdate instance.
       */
      @Override
      public ShellTemplateUpdate build() {
        validate();
        return new ShellTemplateUpdate(this);
      }

      /**
       * Returns the builder instance itself for method chaining.
       *
       * @return The builder instance.
       */
      @Override
      protected Builder self() {
        return this;
      }

      /** Validates the builder fields before building the ShellTemplateUpdate instance. */
      @Override
      protected void validate() {
        super.validate();
        this.newScripts =
            newScripts == null ? Collections.emptyList() : ImmutableList.copyOf(newScripts);
      }
    }
  }

  /** A job template update for spark templates. */
  final class SparkTemplateUpdate extends TemplateUpdate {

    private final String newClassName;

    private final List<String> newJars;

    private final List<String> newFiles;

    private final List<String> newArchives;

    private final Map<String, String> newConfigs;

    private SparkTemplateUpdate(Builder builder) {
      super(builder);
      this.newClassName = builder.newClassName;
      this.newJars = builder.newJars;
      this.newFiles = builder.newFiles;
      this.newArchives = builder.newArchives;
      this.newConfigs = builder.newConfigs;
    }

    /**
     * Get the new class name of the spark job template.
     *
     * @return The new class name of the spark job template.
     */
    public String getNewClassName() {
      return newClassName;
    }

    /**
     * Get the new jars of the spark job template.
     *
     * @return The new jars of the spark job template.
     */
    public List<String> getNewJars() {
      return newJars;
    }

    /**
     * Get the new files of the spark job template.
     *
     * @return The new files of the spark job template.
     */
    public List<String> getNewFiles() {
      return newFiles;
    }

    /**
     * Get the new archives of the spark job template.
     *
     * @return The new archives of the spark job template.
     */
    public List<String> getNewArchives() {
      return newArchives;
    }

    /**
     * Get the new configs of the spark job template.
     *
     * @return The new configs of the spark job template.
     */
    public Map<String, String> getNewConfigs() {
      return newConfigs;
    }

    /**
     * Checks if this SparkTemplateUpdate is equal to another object.
     *
     * @param o The object to compare with.
     * @return true if the objects are equal, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      SparkTemplateUpdate that = (SparkTemplateUpdate) o;
      return Objects.equals(newClassName, that.newClassName)
          && Objects.equals(newJars, that.newJars)
          && Objects.equals(newFiles, that.newFiles)
          && Objects.equals(newArchives, that.newArchives)
          && Objects.equals(newConfigs, that.newConfigs);
    }

    /**
     * Generates a hash code for this SparkTemplateUpdate.
     *
     * @return The hash code.
     */
    @Override
    public int hashCode() {
      return Objects.hash(
          super.hashCode(), newClassName, newJars, newFiles, newArchives, newConfigs);
    }

    /**
     * Creates a new builder for SparkTemplateUpdate.
     *
     * @return A new Builder instance.
     */
    public static Builder builder() {
      return new Builder();
    }

    /** Builder class for constructing SparkTemplateUpdate instances. */
    public static class Builder extends BaseBuilder<Builder, SparkTemplateUpdate> {
      private String newClassName;
      private List<String> newJars;
      private List<String> newFiles;
      private List<String> newArchives;
      private Map<String, String> newConfigs;

      private Builder() {}

      /**
       * Sets the new class name for the spark job template.
       *
       * @param newClassName The new class name to set.
       * @return The builder instance for chaining.
       */
      public Builder withNewClassName(String newClassName) {
        this.newClassName = newClassName;
        return this;
      }

      /**
       * Sets the new jars for the spark job template.
       *
       * @param newJars The new jars to set.
       * @return The builder instance for chaining.
       */
      public Builder withNewJars(List<String> newJars) {
        this.newJars = newJars;
        return this;
      }

      /**
       * Sets the new files for the spark job template.
       *
       * @param newFiles The new files to set.
       * @return The builder instance for chaining.
       */
      public Builder withNewFiles(List<String> newFiles) {
        this.newFiles = newFiles;
        return this;
      }

      /**
       * Sets the new archives for the spark job template.
       *
       * @param newArchives The new archives to set.
       * @return The builder instance for chaining.
       */
      public Builder withNewArchives(List<String> newArchives) {
        this.newArchives = newArchives;
        return this;
      }

      /**
       * Sets the new configs for the spark job template.
       *
       * @param newConfigs The new configs to set.
       * @return The builder instance for chaining.
       */
      public Builder withNewConfigs(Map<String, String> newConfigs) {
        this.newConfigs = newConfigs;
        return this;
      }

      /**
       * Builds the SparkTemplateUpdate instance.
       *
       * @return A new SparkTemplateUpdate instance.
       */
      @Override
      public SparkTemplateUpdate build() {
        validate();
        return new SparkTemplateUpdate(this);
      }

      /**
       * Returns the builder instance itself for method chaining.
       *
       * @return The builder instance.
       */
      @Override
      protected Builder self() {
        return this;
      }

      /** Validates the builder fields before building the SparkTemplateUpdate instance. */
      @Override
      protected void validate() {
        super.validate();
        this.newJars = newJars == null ? Collections.emptyList() : ImmutableList.copyOf(newJars);
        this.newFiles = newFiles == null ? Collections.emptyList() : ImmutableList.copyOf(newFiles);
        this.newArchives =
            newArchives == null ? Collections.emptyList() : ImmutableList.copyOf(newArchives);
        this.newConfigs =
            newConfigs == null ? Collections.emptyMap() : ImmutableMap.copyOf(newConfigs);
      }
    }
  }
}
