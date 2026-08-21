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
package org.apache.gravitino.semantic;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.Evolving;

/**
 * An immutable dataset in a semantic model. A dataset binds a semantic name and optional semantic
 * metadata to a governed Gravitino table or logical view identified by {@link NameIdentifier}.
 */
@Evolving
public final class Dataset {

  private final String name;
  private final NameIdentifier source;

  @Nullable private final String[] primaryKey;
  @Nullable private final String[][] uniqueKeys;
  @Nullable private final String description;
  @Nullable private final AIContext aiContext;
  @Nullable private final Field[] fields;
  @Nullable private final CustomExtension[] customExtensions;

  private Dataset(Builder builder) {
    this.name = builder.name;
    this.source = builder.source;
    this.primaryKey = copyOrNull(builder.primaryKey);
    this.uniqueKeys = copyUniqueKeys(builder.uniqueKeys);
    this.description = builder.description;
    this.aiContext = builder.aiContext;
    this.fields =
        builder.fields == null ? null : Arrays.copyOf(builder.fields, builder.fields.length);
    this.customExtensions =
        builder.customExtensions == null
            ? null
            : Arrays.copyOf(builder.customExtensions, builder.customExtensions.length);
  }

  /**
   * Creates a builder for an immutable {@link Dataset}.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the dataset name.
   *
   * @return The dataset name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the governed table or logical view that backs this dataset.
   *
   * @return The source identifier.
   */
  public NameIdentifier source() {
    return source;
  }

  /**
   * Returns a copy of the primary key columns.
   *
   * @return The primary key columns, or {@code null} if they are not set.
   */
  @Nullable
  public String[] primaryKey() {
    return copyOrNull(primaryKey);
  }

  /**
   * Returns a deep copy of the unique key definitions.
   *
   * @return The unique keys, or {@code null} if they are not set.
   */
  @Nullable
  public String[][] uniqueKeys() {
    return copyUniqueKeys(uniqueKeys);
  }

  /**
   * Returns the dataset description.
   *
   * @return The dataset description, or {@code null} if it is not set.
   */
  @Nullable
  public String description() {
    return description;
  }

  /**
   * Returns the AI context associated with the dataset.
   *
   * @return The AI context, or {@code null} if it is not set.
   */
  @Nullable
  public AIContext aiContext() {
    return aiContext;
  }

  /**
   * Returns a copy of the fields defined by the dataset.
   *
   * @return The fields, or {@code null} if they are not set.
   */
  @Nullable
  public Field[] fields() {
    return fields == null ? null : Arrays.copyOf(fields, fields.length);
  }

  /**
   * Returns a copy of the custom extensions associated with the dataset.
   *
   * @return The custom extensions, or {@code null} if they are not set.
   */
  @Nullable
  public CustomExtension[] customExtensions() {
    return customExtensions == null
        ? null
        : Arrays.copyOf(customExtensions, customExtensions.length);
  }

  /**
   * Compares this dataset with another object for value equality.
   *
   * @param other The object to compare with.
   * @return {@code true} if the objects are equal, otherwise {@code false}.
   */
  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Dataset)) {
      return false;
    }
    Dataset that = (Dataset) other;
    return name.equals(that.name)
        && source.equals(that.source)
        && Arrays.equals(primaryKey, that.primaryKey)
        && Arrays.deepEquals(uniqueKeys, that.uniqueKeys)
        && Objects.equals(description, that.description)
        && Objects.equals(aiContext, that.aiContext)
        && Arrays.equals(fields, that.fields)
        && Arrays.equals(customExtensions, that.customExtensions);
  }

  /**
   * Returns the value-based hash code for this dataset.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    return Objects.hash(
        name,
        source,
        Arrays.hashCode(primaryKey),
        Arrays.deepHashCode(uniqueKeys),
        description,
        aiContext,
        Arrays.hashCode(fields),
        Arrays.hashCode(customExtensions));
  }

  /**
   * Returns a string representation of this dataset.
   *
   * @return The string representation.
   */
  @Override
  public String toString() {
    return "Dataset{"
        + "name='"
        + name
        + '\''
        + ", source="
        + source
        + ", primaryKey="
        + Arrays.toString(primaryKey)
        + ", uniqueKeys="
        + Arrays.deepToString(uniqueKeys)
        + ", description='"
        + description
        + '\''
        + ", aiContext="
        + aiContext
        + ", fields="
        + Arrays.toString(fields)
        + ", customExtensions="
        + Arrays.toString(customExtensions)
        + '}';
  }

  /** A builder for immutable {@link Dataset} values. */
  public static final class Builder {

    private String name;
    private NameIdentifier source;

    @Nullable private String[] primaryKey;
    @Nullable private String[][] uniqueKeys;
    @Nullable private String description;
    @Nullable private AIContext aiContext;
    @Nullable private Field[] fields;
    @Nullable private CustomExtension[] customExtensions;

    private Builder() {}

    /**
     * Sets the dataset name.
     *
     * @param name The non-empty dataset name.
     * @return This builder.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the governed table or logical view that backs this dataset.
     *
     * @param source The source identifier.
     * @return This builder.
     */
    public Builder withSource(NameIdentifier source) {
      this.source = source;
      return this;
    }

    /**
     * Sets the optional primary key columns.
     *
     * @param primaryKey The primary key columns, or {@code null} to leave them unset.
     * @return This builder.
     */
    public Builder withPrimaryKey(@Nullable String[] primaryKey) {
      this.primaryKey = primaryKey;
      return this;
    }

    /**
     * Sets the optional unique key definitions.
     *
     * @param uniqueKeys The unique keys, or {@code null} to leave them unset.
     * @return This builder.
     */
    public Builder withUniqueKeys(@Nullable String[][] uniqueKeys) {
      this.uniqueKeys = uniqueKeys;
      return this;
    }

    /**
     * Sets the optional dataset description.
     *
     * @param description The description, or {@code null} to leave it unset.
     * @return This builder.
     */
    public Builder withDescription(@Nullable String description) {
      this.description = description;
      return this;
    }

    /**
     * Sets the optional AI context.
     *
     * @param aiContext The AI context, or {@code null} to leave it unset.
     * @return This builder.
     */
    public Builder withAIContext(@Nullable AIContext aiContext) {
      this.aiContext = aiContext;
      return this;
    }

    /**
     * Sets the optional semantic fields.
     *
     * @param fields The fields, or {@code null} to leave them unset.
     * @return This builder.
     */
    public Builder withFields(@Nullable Field[] fields) {
      this.fields = fields;
      return this;
    }

    /**
     * Sets the optional custom extensions.
     *
     * @param customExtensions The custom extensions, or {@code null} to leave them unset.
     * @return This builder.
     */
    public Builder withCustomExtensions(@Nullable CustomExtension[] customExtensions) {
      this.customExtensions = customExtensions;
      return this;
    }

    /**
     * Builds an immutable {@link Dataset}.
     *
     * @return The new dataset.
     * @throws IllegalArgumentException If the name is null or empty, the source is null, or an
     *     optional array contains an invalid element.
     */
    public Dataset build() {
      Preconditions.checkArgument(
          name != null && !name.isEmpty(), "name must not be null or empty");
      Preconditions.checkArgument(source != null, "source must not be null");
      SemanticModelDefinition.validateNonEmptyStringElements("primaryKey", primaryKey);
      validateUniqueKeys(uniqueKeys);
      SemanticModelDefinition.validateNoNullElements("fields", fields);
      SemanticModelDefinition.validateNoNullElements("customExtensions", customExtensions);
      return new Dataset(this);
    }
  }

  @Nullable
  private static String[] copyOrNull(@Nullable String[] values) {
    return values == null ? null : Arrays.copyOf(values, values.length);
  }

  @Nullable
  private static String[][] copyUniqueKeys(@Nullable String[][] uniqueKeys) {
    if (uniqueKeys == null) {
      return null;
    }

    String[][] copy = new String[uniqueKeys.length][];
    for (int index = 0; index < uniqueKeys.length; index++) {
      copy[index] = Arrays.copyOf(uniqueKeys[index], uniqueKeys[index].length);
    }
    return copy;
  }

  private static void validateUniqueKeys(@Nullable String[][] uniqueKeys) {
    if (uniqueKeys == null) {
      return;
    }
    for (int index = 0; index < uniqueKeys.length; index++) {
      String[] uniqueKey = uniqueKeys[index];
      Preconditions.checkArgument(
          uniqueKey != null && uniqueKey.length > 0,
          "uniqueKeys[%s] must not be null or empty",
          index);
      SemanticModelDefinition.validateNonEmptyStringElements(
          "uniqueKeys[" + index + "]", uniqueKey);
    }
  }
}
