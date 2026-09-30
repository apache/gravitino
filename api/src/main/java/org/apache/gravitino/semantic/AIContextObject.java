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
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;

/** Structured AI context with optional standard fields and retained custom JSON properties. */
@Evolving
public final class AIContextObject {

  static final int MAX_ADDITIONAL_PROPERTY_NESTING_DEPTH = 100;

  @Nullable private final String instructions;
  @Nullable private final String[] synonyms;
  @Nullable private final String[] examples;
  private final Map<String, Object> additionalProperties;

  private AIContextObject(Builder builder) {
    this.instructions = builder.instructions;
    this.synonyms = SemanticModelDefinition.copyOrNull(builder.synonyms);
    this.examples = SemanticModelDefinition.copyOrNull(builder.examples);
    this.additionalProperties = immutableAdditionalProperties(builder.additionalProperties);
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
   * Returns instructions for AI tools.
   *
   * @return The instructions, or null when not provided.
   */
  @Nullable
  public String instructions() {
    return instructions;
  }

  /**
   * Returns alternative names and terms.
   *
   * @return A defensive copy of the synonyms, or {@code null} when not provided.
   */
  @Nullable
  public String[] synonyms() {
    return SemanticModelDefinition.copyOrNull(synonyms);
  }

  /**
   * Returns sample questions or use cases.
   *
   * @return A defensive copy of the examples, or {@code null} when not provided.
   */
  @Nullable
  public String[] examples() {
    return SemanticModelDefinition.copyOrNull(examples);
  }

  /**
   * Returns custom JSON-compatible properties not represented by the standard fields.
   *
   * <p>The returned map and every nested map or JSON array are unmodifiable. Java arrays supplied
   * to the builder are represented as unmodifiable lists. Integral numbers are represented as
   * {@link BigInteger}, and decimal numbers are represented as {@link BigDecimal}.
   *
   * @return The deeply immutable additional properties, preserving iteration order.
   */
  public Map<String, Object> additionalProperties() {
    return additionalProperties;
  }

  /**
   * Compares this structured AI context with another object.
   *
   * @param other The object to compare.
   * @return {@code true} if the object has the same standard and additional properties.
   */
  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof AIContextObject)) {
      return false;
    }
    AIContextObject that = (AIContextObject) other;
    return Objects.equals(instructions, that.instructions)
        && Arrays.equals(synonyms, that.synonyms)
        && Arrays.equals(examples, that.examples)
        && additionalProperties.equals(that.additionalProperties);
  }

  /**
   * Returns the hash code of this structured AI context.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    int result = Objects.hash(instructions, additionalProperties);
    result = 31 * result + Arrays.hashCode(synonyms);
    result = 31 * result + Arrays.hashCode(examples);
    return result;
  }

  /**
   * Returns a string representation of this structured AI context.
   *
   * @return The string representation.
   */
  @Override
  public String toString() {
    return "AIContextObject{"
        + "instructions='"
        + instructions
        + '\''
        + ", synonyms="
        + Arrays.toString(synonyms)
        + ", examples="
        + Arrays.toString(examples)
        + ", additionalProperties="
        + additionalProperties
        + '}';
  }

  /** A builder for {@link AIContextObject}. */
  public static final class Builder {

    @Nullable private String instructions;
    @Nullable private String[] synonyms;
    @Nullable private String[] examples;
    private Map<String, Object> additionalProperties = Collections.emptyMap();

    private Builder() {}

    /**
     * Sets or clears instructions for AI tools.
     *
     * @param instructions The instructions, or null to leave them unset.
     * @return This builder.
     */
    public Builder withInstructions(@Nullable String instructions) {
      this.instructions = instructions;
      return this;
    }

    /**
     * Sets or clears alternative names and terms.
     *
     * @param synonyms The synonyms, or null to leave them unset.
     * @return This builder.
     */
    public Builder withSynonyms(@Nullable String[] synonyms) {
      this.synonyms = synonyms;
      return this;
    }

    /**
     * Sets or clears sample questions or use cases.
     *
     * @param examples The examples, or null to leave them unset.
     * @return This builder.
     */
    public Builder withExamples(@Nullable String[] examples) {
      this.examples = examples;
      return this;
    }

    /**
     * Sets additional JSON-compatible properties.
     *
     * <p>Values may be null, strings, booleans, JSON-compatible numbers, maps with string keys,
     * lists, or Java arrays. Integral numbers are normalized to {@link BigInteger}, and decimal
     * numbers are normalized to {@link BigDecimal} so their value semantics remain stable across
     * JSON round trips. Nested maps, lists, and arrays may be at most {@value
     * #MAX_ADDITIONAL_PROPERTY_NESTING_DEPTH} levels deep. Property names must not duplicate {@code
     * instructions}, {@code synonyms}, or {@code examples}.
     *
     * @param additionalProperties The additional properties.
     * @return This builder.
     */
    public Builder withAdditionalProperties(Map<String, Object> additionalProperties) {
      this.additionalProperties = additionalProperties;
      return this;
    }

    /**
     * Builds structured AI context.
     *
     * @return The immutable structured AI context.
     * @throws IllegalArgumentException If a string array contains null, the additional properties
     *     are null, a property duplicates a standard field, or a property value is not
     *     JSON-compatible or exceeds the supported nesting depth.
     */
    public AIContextObject build() {
      SemanticModelDefinition.validateNoNullElements("synonyms", synonyms);
      SemanticModelDefinition.validateNoNullElements("examples", examples);
      Preconditions.checkArgument(
          additionalProperties != null, "additionalProperties must not be null");
      return new AIContextObject(this);
    }
  }

  private static Map<String, Object> immutableAdditionalProperties(Map<String, Object> properties) {
    Map<String, Object> result = new LinkedHashMap<>();
    IdentityHashMap<Object, Boolean> visiting = new IdentityHashMap<>();
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      String name = entry.getKey();
      Preconditions.checkArgument(name != null, "additional property name must not be null");
      Preconditions.checkArgument(
          !isStandardProperty(name),
          "additional property must not duplicate standard property: %s",
          name);
      result.put(name, immutableJsonValue(entry.getValue(), name, visiting, 0));
    }
    return Collections.unmodifiableMap(result);
  }

  private static Object immutableJsonValue(
      @Nullable Object value,
      String path,
      IdentityHashMap<Object, Boolean> visiting,
      int containerDepth) {
    if (value == null || value instanceof String || value instanceof Boolean) {
      return value;
    }
    if (value instanceof Number) {
      return canonicalizeJsonNumber((Number) value, path);
    }
    if (value instanceof Map) {
      return immutableJsonMap((Map<?, ?>) value, path, visiting, containerDepth + 1);
    }
    if (value instanceof List) {
      return immutableJsonList((List<?>) value, path, visiting, containerDepth + 1);
    }
    if (value.getClass().isArray()) {
      return immutableJsonArray(value, path, visiting, containerDepth + 1);
    }
    throw new IllegalArgumentException(
        String.format(
            "Additional property %s has non-JSON-compatible value type: %s",
            path, value.getClass().getName()));
  }

  private static Map<String, Object> immutableJsonMap(
      Map<?, ?> value, String path, IdentityHashMap<Object, Boolean> visiting, int containerDepth) {
    enterContainer(value, path, visiting, containerDepth);
    try {
      Map<String, Object> result = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : value.entrySet()) {
        Preconditions.checkArgument(
            entry.getKey() instanceof String,
            "Additional property %s contains a map key that is not a string",
            path);
        String key = (String) entry.getKey();
        result.put(
            key, immutableJsonValue(entry.getValue(), path + "." + key, visiting, containerDepth));
      }
      return Collections.unmodifiableMap(result);
    } finally {
      visiting.remove(value);
    }
  }

  private static List<Object> immutableJsonList(
      List<?> value, String path, IdentityHashMap<Object, Boolean> visiting, int containerDepth) {
    enterContainer(value, path, visiting, containerDepth);
    try {
      List<Object> result = new ArrayList<>(value.size());
      for (int index = 0; index < value.size(); index++) {
        result.add(
            immutableJsonValue(
                value.get(index), path + "[" + index + "]", visiting, containerDepth));
      }
      return Collections.unmodifiableList(result);
    } finally {
      visiting.remove(value);
    }
  }

  private static List<Object> immutableJsonArray(
      Object value, String path, IdentityHashMap<Object, Boolean> visiting, int containerDepth) {
    enterContainer(value, path, visiting, containerDepth);
    try {
      int length = Array.getLength(value);
      List<Object> result = new ArrayList<>(length);
      for (int index = 0; index < length; index++) {
        result.add(
            immutableJsonValue(
                Array.get(value, index), path + "[" + index + "]", visiting, containerDepth));
      }
      return Collections.unmodifiableList(result);
    } finally {
      visiting.remove(value);
    }
  }

  private static Number canonicalizeJsonNumber(Number value, String path) {
    boolean supported =
        value instanceof Byte
            || value instanceof Short
            || value instanceof Integer
            || value instanceof Long
            || value instanceof Float
            || value instanceof Double
            || value instanceof BigInteger
            || value instanceof BigDecimal;
    Preconditions.checkArgument(
        supported,
        "Additional property %s has non-JSON-compatible number type: %s",
        path,
        value.getClass().getName());
    if (value instanceof BigInteger || value instanceof BigDecimal) {
      return value;
    }
    if (value instanceof Byte
        || value instanceof Short
        || value instanceof Integer
        || value instanceof Long) {
      return BigInteger.valueOf(value.longValue());
    }
    if (value instanceof Double) {
      double number = value.doubleValue();
      Preconditions.checkArgument(
          !Double.isNaN(number) && !Double.isInfinite(number),
          "Additional property %s must contain a finite number",
          path);
    } else {
      float number = value.floatValue();
      Preconditions.checkArgument(
          !Float.isNaN(number) && !Float.isInfinite(number),
          "Additional property %s must contain a finite number",
          path);
    }
    return new BigDecimal(value.toString());
  }

  private static void enterContainer(
      Object value, String path, IdentityHashMap<Object, Boolean> visiting, int containerDepth) {
    Preconditions.checkArgument(
        containerDepth <= MAX_ADDITIONAL_PROPERTY_NESTING_DEPTH,
        "Additional property %s exceeds maximum nesting depth of %s",
        path,
        MAX_ADDITIONAL_PROPERTY_NESTING_DEPTH);
    Preconditions.checkArgument(
        !visiting.containsKey(value), "Additional property %s contains a cyclic value", path);
    visiting.put(value, Boolean.TRUE);
  }

  private static boolean isStandardProperty(String name) {
    return "instructions".equals(name) || "synonyms".equals(name) || "examples".equals(name);
  }
}
