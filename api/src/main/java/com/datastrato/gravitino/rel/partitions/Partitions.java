/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.partitions;

import com.datastrato.gravitino.rel.expressions.literals.Literal;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/** The helper class for partition expressions. */
public class Partitions {

  /**
   * Creates a range partition.
   *
   * @param name The name of the partition.
   * @param upper The upper bound of the partition.
   * @param lower The lower bound of the partition.
   * @param properties The properties of the partition.
   * @return The created partition.
   */
  public static Partition range(
      String name, Literal<?> upper, Literal<?> lower, Map<String, String> properties) {
    return new RangePartitionImpl(name, upper, lower, properties);
  }

  /**
   * Creates a list partition.
   *
   * @param name The name of the partition.
   * @param lists The values of the list partition.
   * @param properties The properties of the partition.
   * @return The created partition.
   */
  public static Partition list(String name, Literal<?>[][] lists, Map<String, String> properties) {
    return new ListPartitionImpl(name, lists, properties);
  }

  /**
   * Creates an identity partition.
   *
   * <p>The {@code values} must correspond to the {@code fieldNames}.
   *
   * @param name The name of the partition.
   * @param fieldNames The field names of the identity partition.
   * @param values The value of the identity partition.
   * @param properties The properties of the partition.
   * @return The created partition.
   */
  public static Partition identity(
      String name, String[][] fieldNames, Literal<?>[] values, Map<String, String> properties) {
    return new IdentityPartitionImpl(name, fieldNames, values, properties);
  }

  /**
   * Creates an identity partition whose name will be automatically generated.
   *
   * @param fieldNames The field names of the identity partition.
   * @param values The values of the identity partition.
   * @return The created partition.
   */
  public static Partition identity(String[][] fieldNames, Literal<?>[] values) {
    return identity(null, fieldNames, values, null);
  }

  /** Represents a result of range partitioning. */
  private static class RangePartitionImpl implements RangePartition {
    private final String name;
    private final Literal<?> upper;
    private final Literal<?> lower;

    private final Map<String, String> properties;

    private RangePartitionImpl(
        String name, Literal<?> upper, Literal<?> lower, Map<String, String> properties) {
      this.name = name;
      this.properties = properties;
      this.upper = upper;
      this.lower = lower;
    }

    /** @return The upper bound of the partition. */
    @Override
    public Literal<?> upper() {
      return upper;
    }

    /** @return The lower bound of the partition. */
    @Override
    public Literal<?> lower() {
      return lower;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RangePartitionImpl)) {
        return false;
      }
      RangePartitionImpl that = (RangePartitionImpl) o;
      return Objects.equals(name, that.name)
          && Objects.equals(upper, that.upper)
          && Objects.equals(lower, that.lower)
          && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, upper, lower, properties);
    }
  }

  /** Represents a result of list partitioning. */
  private static class ListPartitionImpl implements ListPartition {
    private final String name;
    private final Literal<?>[][] lists;

    private final Map<String, String> properties;

    private ListPartitionImpl(String name, Literal<?>[][] lists, Map<String, String> properties) {
      this.name = name;
      this.properties = properties;
      this.lists = lists;
    }

    /** @return The values of the list partition. */
    @Override
    public Literal<?>[][] lists() {
      return lists;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ListPartitionImpl)) {
        return false;
      }
      ListPartitionImpl that = (ListPartitionImpl) o;
      return Objects.equals(name, that.name)
          && Arrays.deepEquals(lists, that.lists)
          && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(name, properties);
      result = 31 * result + Arrays.deepHashCode(lists);
      return result;
    }
  }

  /** Represents a result of identity partitioning. */
  private static class IdentityPartitionImpl implements IdentityPartition {
    private final String name;
    private final String[][] fieldNames;
    private final Literal<?>[] values;
    private final Map<String, String> properties;

    private IdentityPartitionImpl(
        String name, String[][] fieldNames, Literal<?>[] values, Map<String, String> properties) {
      this.name = name;
      this.fieldNames = fieldNames;
      this.values = values;
      this.properties = properties;
    }

    /** @return The field names of the identity partition. */
    @Override
    public String[][] fieldNames() {
      return fieldNames;
    }

    /** @return The values of the identity partition. */
    @Override
    public Literal<?>[] values() {
      return values;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof IdentityPartitionImpl)) {
        return false;
      }
      IdentityPartitionImpl that = (IdentityPartitionImpl) o;
      return Objects.equals(name, that.name)
          && Arrays.deepEquals(fieldNames, that.fieldNames)
          && Arrays.equals(values, that.values)
          && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(name, properties);
      result = 31 * result + Arrays.deepHashCode(fieldNames);
      result = 31 * result + Arrays.hashCode(values);
      return result;
    }
  }

  private Partitions() {}
}
