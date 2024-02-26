/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitions;

import com.datastrato.gravitino.dto.rel.expressions.LiteralDTO;
import com.datastrato.gravitino.rel.partitions.IdentityPartition;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/** Data transfer object representing an identity partition. */
public class IdentityPartitionDTO implements PartitionDTO, IdentityPartition {

  private final String name;
  private final String[][] fieldNames;
  private final LiteralDTO[] values;
  private final Map<String, String> properties;

  /** @return A builder instance for {@link IdentityPartitionDTO}. */
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

  /** @return The name of the partition. */
  @Override
  public String name() {
    return name;
  }

  /** @return The field names. */
  @Override
  public String[][] fieldNames() {
    return fieldNames;
  }

  /** @return The values. */
  @Override
  public LiteralDTO[] values() {
    return values;
  }

  /** @return The properties. */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /** @return The type of the partition. */
  @Override
  public Type type() {
    return Type.IDENTITY;
  }

  /** Builder for {@link IdentityPartitionDTO}. */
  public static class Builder {
    private String name;
    private String[][] fieldNames;
    private LiteralDTO[] values;
    private Map<String, String> properties;

    /**
     * Set the name of the partition.
     *
     * @param name The name of the partition.
     * @return The builder.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Set the field names for the partition.
     *
     * @param fieldNames The field names.
     * @return The builder.
     */
    public Builder withFieldNames(String[][] fieldNames) {
      this.fieldNames = fieldNames;
      return this;
    }

    /**
     * Set the values for the partition.
     *
     * @param values The values.
     * @return The builder.
     */
    public Builder withValues(LiteralDTO[] values) {
      this.values = values;
      return this;
    }

    /**
     * Set the properties for the partition.
     *
     * @param properties The properties.
     * @return The builder.
     */
    public Builder withProperties(Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    /**
     * Build the partition.
     *
     * @return The partition.
     */
    public IdentityPartitionDTO build() {
      return new IdentityPartitionDTO(name, fieldNames, values, properties);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof IdentityPartitionDTO)) {
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
