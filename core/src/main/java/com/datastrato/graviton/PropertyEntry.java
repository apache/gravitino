/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import com.google.common.base.Preconditions;
import java.util.function.Function;
import lombok.Getter;
import org.apache.logging.log4j.util.Strings;

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
    Preconditions.checkArgument(Strings.isNotBlank(name), "name cannot be null or empty");
    Preconditions.checkArgument(
        Strings.isNotBlank(description), "description cannot be null or empty");
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

    public Builder<T> name(String name) {
      this.name = name;
      return this;
    }

    public Builder<T> description(String description) {
      this.description = description;
      return this;
    }

    public Builder<T> required(boolean required) {
      this.required = required;
      return this;
    }

    public Builder<T> immutable(boolean immutable) {
      this.immutable = immutable;
      return this;
    }

    public Builder<T> javaType(Class<T> javaType) {
      this.javaType = javaType;
      return this;
    }

    public Builder<T> defaultValue(T defaultValue) {
      this.defaultValue = defaultValue;
      return this;
    }

    public Builder<T> decoder(Function<String, T> decoder) {
      this.decoder = decoder;
      return this;
    }

    public Builder<T> encoder(Function<T, String> encoder) {
      this.encoder = encoder;
      return this;
    }

    public Builder<T> hidden(boolean hidden) {
      this.hidden = hidden;
      return this;
    }

    public Builder<T> reserved(boolean reserved) {
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

  public static PropertyEntry<String> stringProperty(
      String name,
      String description,
      boolean required,
      boolean immutable,
      String defaultValue,
      boolean hidden,
      boolean reserved) {
    return new Builder<String>()
        .name(name)
        .description(description)
        .required(required)
        .immutable(immutable)
        .javaType(String.class)
        .defaultValue(defaultValue)
        .decoder(Function.identity())
        .encoder(Function.identity())
        .hidden(hidden)
        .reserved(reserved)
        .build();
  }
}
