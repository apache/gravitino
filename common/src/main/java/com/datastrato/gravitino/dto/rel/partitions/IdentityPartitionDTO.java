/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitions;

import com.datastrato.gravitino.dto.rel.expressions.LiteralDTO;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class IdentityPartitionDTO implements PartitionDTO {

  private final String name;
  private final String[][] fieldNames;
  private final LiteralDTO[] values;
  private final Map<String, String> properties;

  public static Builder builder() {
    return new Builder();
  }

  private IdentityPartitionDTO() {
    this(null, null, null, null);
  }

  private IdentityPartitionDTO(
      String name, String[][] fieldNames, LiteralDTO[] values, Map<String, String> properties) {
    this.name = name;
    this.fieldNames = fieldNames;
    this.values = values;
    this.properties = properties;
  }

  @Override
  public String name() {
    return name;
  }

  public String[][] fieldNames() {
    return fieldNames;
  }

  public LiteralDTO[] values() {
    return values;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public Type type() {
    return Type.IDENTITY;
  }

  public static class Builder {
    private String name;
    private String[][] fieldNames;
    private LiteralDTO[] values;
    private Map<String, String> properties;

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withFieldNames(String[][] fieldNames) {
      this.fieldNames = fieldNames;
      return this;
    }

    public Builder withValues(LiteralDTO[] values) {
      this.values = values;
      return this;
    }

    public Builder withProperties(Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    public IdentityPartitionDTO build() {
      return new IdentityPartitionDTO(name, fieldNames, values, properties);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IdentityPartitionDTO that = (IdentityPartitionDTO) o;
    return Objects.equals(name, that.name)
        && Arrays.deepEquals(fieldNames, that.fieldNames)
        && Arrays.equals(values, that.values)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, Arrays.deepHashCode(fieldNames), Arrays.hashCode(values), properties);
  }
}
