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
package org.apache.gravitino.connector;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

import com.google.common.base.Preconditions;
import java.util.EnumSet;
import java.util.function.Function;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@Getter
public final class PropertyEntry<T> {
  private final String name;
  private final String description;
  private final boolean required;
  private final boolean immutable;
  private final Class<T> javaType;
  private final T defaultValue;
  private final Function<String, T> decoder;
  private final Function<T, String> encoder;
  private final boolean hidden;
  private final boolean reserved;

  /**
   * @param name The name of the property
   * @param description Describe the purpose of this property
   * @param required Whether this property is required. If true, the property must be set when
   *     creating a table
   * @param immutable Whether this property is immutable. If true, the property cannot be changed by
   *     user after the table is created
   * @param javaType The java type of the property
   * @param defaultValue Non-required property can have a default value
   * @param decoder Decode the string value to the java type
   * @param encoder Encode the java type to the string value
   * @param hidden Whether this property is hidden from user, such as password
   * @param reserved This property is reserved and cannot be set by user
   */
  private PropertyEntry(
      String name,
      String description,
      boolean required,
      boolean immutable,
      Class<T> javaType,
      T defaultValue,
      Function<String, T> decoder,
      Function<T, String> encoder,
      boolean hidden,
      boolean reserved) {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be null or empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(description), "description cannot be null or empty");
    Preconditions.checkArgument(javaType != null, "javaType cannot be null");
    Preconditions.checkArgument(decoder != null, "decoder cannot be null");
    Preconditions.checkArgument(encoder != null, "encoder cannot be null");

    Preconditions.checkArgument(
        !required || defaultValue == null, "defaultValue cannot be set for required property");
    Preconditions.checkArgument(!required || !reserved, "required property cannot be reserved");
    Preconditions.checkArgument(!reserved || immutable, "reserved property must be immutable");

    this.name = name;
    this.description = description;
    this.required = required;
    this.immutable = immutable;
    this.javaType = javaType;
    this.defaultValue = defaultValue;
    this.decoder = decoder;
    this.encoder = encoder;
    this.hidden = hidden;
    this.reserved = reserved;
  }

  public static class Builder<T> {
    private String name;
    private String description;
    private boolean required;
    private boolean immutable;
    private Class<T> javaType;
    private T defaultValue;
    private Function<String, T> decoder;
    private Function<T, String> encoder;
    private boolean hidden;
    private boolean reserved;

    public Builder<T> withName(String name) {
      this.name = name;
      return this;
    }

    public Builder<T> withDescription(String description) {
      this.description = description;
      return this;
    }

    public Builder<T> withRequired(boolean required) {
      this.required = required;
      return this;
    }

    public Builder<T> withImmutable(boolean immutable) {
      this.immutable = immutable;
      return this;
    }

    public Builder<T> withJavaType(Class<T> javaType) {
      this.javaType = javaType;
      return this;
    }

    public Builder<T> withDefaultValue(T defaultValue) {
      this.defaultValue = defaultValue;
      return this;
    }

    public Builder<T> withDecoder(Function<String, T> decoder) {
      this.decoder = decoder;
      return this;
    }

    public Builder<T> withEncoder(Function<T, String> encoder) {
      this.encoder = encoder;
      return this;
    }

    public Builder<T> withHidden(boolean hidden) {
      this.hidden = hidden;
      return this;
    }

    public Builder<T> withReserved(boolean reserved) {
      this.reserved = reserved;
      return this;
    }

    public PropertyEntry<T> build() {
      return new PropertyEntry<T>(
          name,
          description,
          required,
          immutable,
          javaType,
          defaultValue,
          decoder,
          encoder,
          hidden,
          reserved);
    }
  }

  public T decode(String value) {
    return decoder.apply(value);
  }

  public static PropertyEntry<String> stringPropertyEntry(
      String name,
      String description,
      boolean required,
      boolean immutable,
      String defaultValue,
      boolean hidden,
      boolean reserved) {
    return new Builder<String>()
        .withName(name)
        .withDescription(description)
        .withRequired(required)
        .withImmutable(immutable)
        .withJavaType(String.class)
        .withDefaultValue(defaultValue)
        .withDecoder(Function.identity())
        .withEncoder(Function.identity())
        .withHidden(hidden)
        .withReserved(reserved)
        .build();
  }

  public static PropertyEntry<Long> longPropertyEntry(
      String name,
      String description,
      boolean required,
      boolean immutable,
      long defaultValue,
      boolean hidden,
      boolean reserved) {
    return new Builder<Long>()
        .withName(name)
        .withDescription(description)
        .withRequired(required)
        .withImmutable(immutable)
        .withJavaType(Long.class)
        .withDefaultValue(defaultValue)
        .withDecoder(Long::parseLong)
        .withEncoder(String::valueOf)
        .withHidden(hidden)
        .withReserved(reserved)
        .build();
  }

  public static PropertyEntry<Integer> integerPropertyEntry(
      String name,
      String description,
      boolean required,
      boolean immutable,
      Integer defaultValue,
      boolean hidden,
      boolean reserved) {
    return new Builder<Integer>()
        .withName(name)
        .withDescription(description)
        .withRequired(required)
        .withImmutable(immutable)
        .withJavaType(Integer.class)
        .withDefaultValue(defaultValue)
        .withDecoder(Integer::parseInt)
        .withEncoder(String::valueOf)
        .withHidden(hidden)
        .withReserved(reserved)
        .build();
  }

  public static PropertyEntry<Short> shortPropertyEntry(
      String name,
      String description,
      boolean required,
      boolean immutable,
      Short defaultValue,
      boolean hidden,
      boolean reserved) {
    return new Builder<Short>()
        .withName(name)
        .withDescription(description)
        .withRequired(required)
        .withImmutable(immutable)
        .withJavaType(Short.class)
        .withDefaultValue(defaultValue)
        .withDecoder(Short::parseShort)
        .withEncoder(String::valueOf)
        .withHidden(hidden)
        .withReserved(reserved)
        .build();
  }

  public static PropertyEntry<String> stringReservedPropertyEntry(
      String name, String description, boolean hidden) {
    return stringPropertyEntry(name, description, false, true, null, hidden, true);
  }

  public static PropertyEntry<Boolean> booleanReservedPropertyEntry(
      String name, String description, boolean defaultValue, boolean hidden) {
    return booleanPropertyEntry(name, description, false, true, defaultValue, hidden, true);
  }

  public static PropertyEntry<Boolean> booleanPropertyEntry(
      String name,
      String description,
      boolean required,
      boolean immutable,
      Boolean defaultValue,
      boolean hidden,
      boolean reserved) {
    return new Builder<Boolean>()
        .withName(name)
        .withDescription(description)
        .withRequired(required)
        .withImmutable(immutable)
        .withJavaType(Boolean.class)
        .withDefaultValue(defaultValue)
        .withDecoder(Boolean::valueOf)
        .withEncoder(b -> Boolean.toString(b).toUpperCase())
        .withHidden(hidden)
        .withReserved(reserved)
        .build();
  }

  public static PropertyEntry<String> stringRequiredPropertyEntry(
      String name, String description, boolean immutable, boolean hidden) {
    return stringPropertyEntry(name, description, true, immutable, null, hidden, false);
  }

  public static PropertyEntry<String> stringOptionalPropertyEntry(
      String name, String description, boolean immutable, String defaultValue, boolean hidden) {
    return stringPropertyEntry(name, description, false, immutable, defaultValue, hidden, false);
  }

  public static PropertyEntry<String> stringMutablePropertyEntry(
      String name, String description, boolean required, String defaultValue, boolean hidden) {
    return stringPropertyEntry(name, description, required, false, defaultValue, hidden, false);
  }

  public static PropertyEntry<Short> shortOptionalPropertyEntry(
      String name, String description, boolean immutable, Short defaultValue, boolean hidden) {
    return shortPropertyEntry(name, description, false, immutable, defaultValue, hidden, false);
  }

  public static PropertyEntry<Integer> integerOptionalPropertyEntry(
      String name, String description, boolean immutable, Integer defaultValue, boolean hidden) {
    return integerPropertyEntry(name, description, false, immutable, defaultValue, hidden, false);
  }

  public static PropertyEntry<Long> longOptionalPropertyEntry(
      String name, String description, boolean immutable, long defaultValue, boolean hidden) {
    return longPropertyEntry(name, description, false, immutable, defaultValue, hidden, false);
  }

  public static PropertyEntry<String> stringImmutablePropertyEntry(
      String name,
      String description,
      boolean required,
      String defaultValue,
      boolean hidden,
      boolean reserved) {
    return stringPropertyEntry(name, description, required, true, defaultValue, hidden, reserved);
  }

  public static <T extends Enum<T>> PropertyEntry<T> enumPropertyEntry(
      String name,
      String description,
      boolean required,
      boolean immutable,
      Class<T> javaType,
      T defaultValue,
      boolean hidden,
      boolean reserved) {
    String validValues =
        EnumSet.allOf(javaType).stream()
            .map(Enum::name)
            .map(String::toLowerCase)
            .collect(joining(", ", "[", "]"));
    return new Builder<T>()
        .withName(name)
        .withDescription(description)
        .withRequired(required)
        .withImmutable(immutable)
        .withJavaType(javaType)
        .withDefaultValue(defaultValue)
        .withDecoder(
            value -> {
              try {
                return Enum.valueOf(javaType, value.toUpperCase());
              } catch (IllegalArgumentException | NullPointerException e) {
                throw new IllegalArgumentException(
                    format("Invalid value [%s]. Valid values: %s", value, validValues), e);
              }
            })
        .withEncoder(e -> e.name().toLowerCase())
        .withHidden(hidden)
        .withReserved(reserved)
        .build();
  }

  public static <T extends Enum<T>> PropertyEntry<T> enumImmutablePropertyEntry(
      String name,
      String description,
      boolean required,
      Class<T> javaType,
      T defaultValue,
      boolean hidden,
      boolean reserved) {
    return enumPropertyEntry(
        name, description, required, true, javaType, defaultValue, hidden, reserved);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("PropertyEntry{");
    sb.append("name='").append(name).append('\'');
    sb.append(", description='").append(description).append('\'');
    sb.append(", required=").append(required);
    sb.append(", immutable=").append(immutable);
    sb.append(", javaType=").append(javaType);
    sb.append(", defaultValue=").append(defaultValue);
    sb.append(", hidden=").append(hidden);
    sb.append(", reserved=").append(reserved);
    sb.append('}');
    return sb.toString();
  }
}
