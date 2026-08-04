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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Unstable;

/** AI-oriented context represented either as text or as a structured object. */
@Unstable
public final class AIContext {

  private static final List<String> RESERVED_PROPERTIES =
      java.util.Arrays.asList("instructions", "synonyms", "examples");

  @Nullable private final String text;
  @Nullable private final String instructions;
  private final List<String> synonyms;
  private final List<String> examples;
  private final Map<String, Object> additionalProperties;

  private AIContext(
      @Nullable String text,
      @Nullable String instructions,
      List<String> synonyms,
      List<String> examples,
      Map<String, Object> additionalProperties) {
    this.text = text;
    this.instructions = instructions;
    this.synonyms = synonyms;
    this.examples = examples;
    this.additionalProperties = additionalProperties;
  }

  /**
   * Creates text-form AI context.
   *
   * @param text The context text.
   * @return Text-form AI context.
   */
  public static AIContext of(String text) {
    return new AIContext(
        SemanticModelUtils.requireNonBlank(text, "text"),
        null,
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyMap());
  }

  /**
   * Creates a builder for structured AI context.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns whether this context uses the text form.
   *
   * @return {@code true} for text-form context.
   */
  public boolean isText() {
    return text != null;
  }

  /**
   * Returns the text-form value.
   *
   * @return The text, or {@code null} for structured context.
   */
  @Nullable
  public String text() {
    return text;
  }

  /**
   * Returns structured instructions.
   *
   * @return The instructions, or {@code null} when absent or when this is text-form context.
   */
  @Nullable
  public String instructions() {
    return instructions;
  }

  /**
   * Returns structured synonyms.
   *
   * @return An immutable list of synonyms.
   */
  public List<String> synonyms() {
    return synonyms;
  }

  /**
   * Returns structured examples.
   *
   * @return An immutable list of examples.
   */
  public List<String> examples() {
    return examples;
  }

  /**
   * Returns additional structured properties retained from the OSI model.
   *
   * @return An immutable map of additional properties.
   */
  public Map<String, Object> additionalProperties() {
    return additionalProperties;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof AIContext)) {
      return false;
    }
    AIContext that = (AIContext) other;
    return Objects.equals(text, that.text)
        && Objects.equals(instructions, that.instructions)
        && Objects.equals(synonyms, that.synonyms)
        && Objects.equals(examples, that.examples)
        && Objects.equals(additionalProperties, that.additionalProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(text, instructions, synonyms, examples, additionalProperties);
  }

  @Override
  public String toString() {
    return "AIContext{"
        + "text='"
        + text
        + '\''
        + ", instructions='"
        + instructions
        + '\''
        + ", synonyms="
        + synonyms
        + ", examples="
        + examples
        + ", additionalProperties="
        + additionalProperties
        + '}';
  }

  /** Builder for structured {@link AIContext}. */
  public static final class Builder {
    @Nullable private String instructions;
    private List<String> synonyms;
    private List<String> examples;
    private Map<String, Object> additionalProperties;

    private Builder() {}

    /**
     * Sets instructions for consumers of the semantic definition.
     *
     * @param instructions The instructions.
     * @return This builder.
     */
    public Builder withInstructions(@Nullable String instructions) {
      this.instructions = instructions;
      return this;
    }

    /**
     * Sets synonyms for the semantic definition.
     *
     * @param synonyms The synonyms.
     * @return This builder.
     */
    public Builder withSynonyms(List<String> synonyms) {
      this.synonyms = synonyms;
      return this;
    }

    /**
     * Sets examples for the semantic definition.
     *
     * @param examples The examples.
     * @return This builder.
     */
    public Builder withExamples(List<String> examples) {
      this.examples = examples;
      return this;
    }

    /**
     * Sets additional structured properties.
     *
     * @param additionalProperties Additional OSI properties.
     * @return This builder.
     */
    public Builder withAdditionalProperties(Map<String, Object> additionalProperties) {
      this.additionalProperties = additionalProperties;
      return this;
    }

    /**
     * Builds structured AI context.
     *
     * @return The structured AI context.
     */
    public AIContext build() {
      Map<String, Object> copiedProperties = SemanticModelUtils.immutableMap(additionalProperties);
      Preconditions.checkArgument(
          copiedProperties.keySet().stream().noneMatch(RESERVED_PROPERTIES::contains),
          "additionalProperties must not redefine instructions, synonyms, or examples");
      return new AIContext(
          null,
          instructions,
          SemanticModelUtils.immutableList(synonyms, "synonyms"),
          SemanticModelUtils.immutableList(examples, "examples"),
          copiedProperties);
    }
  }
}
