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
package org.apache.gravitino.rel.metric;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.Unstable;

/** A dataset participating in an OSI-compatible semantic model. */
@Unstable
public final class Dataset {

  private final String name;
  private final NameIdentifier source;
  private final List<String> primaryKey;
  private final List<List<String>> uniqueKeys;
  @Nullable private final String description;
  @Nullable private final AIContext aiContext;
  private final List<Field> fields;
  private final List<CustomExtension> customExtensions;

  private Dataset(
      String name,
      NameIdentifier source,
      List<String> primaryKey,
      List<List<String>> uniqueKeys,
      @Nullable String description,
      @Nullable AIContext aiContext,
      List<Field> fields,
      List<CustomExtension> customExtensions) {
    this.name = name;
    this.source = source;
    this.primaryKey = primaryKey;
    this.uniqueKeys = uniqueKeys;
    this.description = description;
    this.aiContext = aiContext;
    this.fields = fields;
    this.customExtensions = customExtensions;
  }

  /**
   * @return A new dataset builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * @return The dataset name.
   */
  public String name() {
    return name;
  }

  /**
   * @return The catalog and schema-qualified source identifier.
   */
  public NameIdentifier source() {
    return source;
  }

  /**
   * @return An immutable primary-key column list.
   */
  public List<String> primaryKey() {
    return primaryKey;
  }

  /**
   * @return An immutable list of unique-key column lists.
   */
  public List<List<String>> uniqueKeys() {
    return uniqueKeys;
  }

  /**
   * @return The dataset description, or {@code null}.
   */
  @Nullable
  public String description() {
    return description;
  }

  /**
   * @return The AI context, or {@code null}.
   */
  @Nullable
  public AIContext aiContext() {
    return aiContext;
  }

  /**
   * @return An immutable list of fields.
   */
  public List<Field> fields() {
    return fields;
  }

  /**
   * @return An immutable list of custom extensions.
   */
  public List<CustomExtension> customExtensions() {
    return customExtensions;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Dataset)) {
      return false;
    }
    Dataset that = (Dataset) other;
    return Objects.equals(name, that.name)
        && Objects.equals(source, that.source)
        && Objects.equals(primaryKey, that.primaryKey)
        && Objects.equals(uniqueKeys, that.uniqueKeys)
        && Objects.equals(description, that.description)
        && Objects.equals(aiContext, that.aiContext)
        && Objects.equals(fields, that.fields)
        && Objects.equals(customExtensions, that.customExtensions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name, source, primaryKey, uniqueKeys, description, aiContext, fields, customExtensions);
  }

  @Override
  public String toString() {
    return "Dataset{"
        + "name='"
        + name
        + '\''
        + ", source="
        + source
        + ", primaryKey="
        + primaryKey
        + ", uniqueKeys="
        + uniqueKeys
        + ", description='"
        + description
        + '\''
        + ", aiContext="
        + aiContext
        + ", fields="
        + fields
        + ", customExtensions="
        + customExtensions
        + '}';
  }

  /** Builder for {@link Dataset}. */
  public static final class Builder {
    private String name;
    private NameIdentifier source;
    private List<String> primaryKey;
    private List<List<String>> uniqueKeys;
    @Nullable private String description;
    @Nullable private AIContext aiContext;
    private List<Field> fields;
    private List<CustomExtension> customExtensions;

    private Builder() {}

    /**
     * Sets the dataset name.
     *
     * @param name The dataset name.
     * @return This builder.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the catalog and schema-qualified source.
     *
     * @param source The source identifier.
     * @return This builder.
     */
    public Builder withSource(NameIdentifier source) {
      this.source = source;
      return this;
    }

    /**
     * Sets the primary-key columns.
     *
     * @param primaryKey The primary-key columns.
     * @return This builder.
     */
    public Builder withPrimaryKey(List<String> primaryKey) {
      this.primaryKey = primaryKey;
      return this;
    }

    /**
     * Sets the unique-key column lists.
     *
     * @param uniqueKeys The unique-key column lists.
     * @return This builder.
     */
    public Builder withUniqueKeys(List<List<String>> uniqueKeys) {
      this.uniqueKeys = uniqueKeys;
      return this;
    }

    /**
     * Sets the dataset description.
     *
     * @param description The dataset description.
     * @return This builder.
     */
    public Builder withDescription(@Nullable String description) {
      this.description = description;
      return this;
    }

    /**
     * Sets the AI context.
     *
     * @param aiContext The AI context.
     * @return This builder.
     */
    public Builder withAIContext(@Nullable AIContext aiContext) {
      this.aiContext = aiContext;
      return this;
    }

    /**
     * Sets the dataset fields.
     *
     * @param fields The dataset fields.
     * @return This builder.
     */
    public Builder withFields(List<Field> fields) {
      this.fields = fields;
      return this;
    }

    /**
     * Sets the custom extensions.
     *
     * @param customExtensions The custom extensions.
     * @return This builder.
     */
    public Builder withCustomExtensions(List<CustomExtension> customExtensions) {
      this.customExtensions = customExtensions;
      return this;
    }

    /**
     * @return The dataset.
     */
    public Dataset build() {
      Preconditions.checkArgument(source != null, "source must not be null");
      Preconditions.checkArgument(
          source.namespace().length() == 2,
          "source namespace must contain catalog and schema: %s",
          source);
      return new Dataset(
          SemanticModelUtils.requireNonBlank(name, "name"),
          source,
          SemanticModelUtils.immutableList(primaryKey, "primaryKey"),
          SemanticModelUtils.immutableNestedStringList(uniqueKeys, "uniqueKeys"),
          description,
          aiContext,
          SemanticModelUtils.immutableList(fields, "fields"),
          SemanticModelUtils.immutableList(customExtensions, "customExtensions"));
    }
  }
}
