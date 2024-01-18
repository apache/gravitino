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
   * Creates a range partition. For example, for range partition `PARTITION p20200321 VALUES LESS
   * THAN ("2020-03-22")`, its upper bound is "2020-03-22" and its lower bound is null.
   *
   * @param name The name of the partition.
   * @param upper The upper bound of the partition.
   * @param lower The lower bound of the partition.
   * @param properties The properties of the partition.
   * @return The created partition.
   */
  public static RangePartition range(
      String name, Literal<?> upper, Literal<?> lower, Map<String, String> properties) {
    return new RangePartition(name, upper, lower, properties);
  }

  /**
   * Creates a list partition. For example, for list partition
   *
   * <pre>
   * `PARTITION p202204_California VALUES IN (
   *   ("2022-04-01", "Los Angeles"),
   *   ("2022-04-01", "San Francisco")
   * )`
   * </pre>
   *
   * its name is "p202204_California" and lists are [["2022-04-01","Los Angeles"], ["2022-04-01",
   * "San Francisco"]].
   *
   * @param name The name of the partition.
   * @param lists The values of the list partition.
   * @param properties The properties of the partition.
   * @return The created partition.
   */
  public static ListPartition list(
      String name, Literal<?>[][] lists, Map<String, String> properties) {
    return new ListPartition(name, lists, properties);
  }

  /**
   * Creates an identity partition. For example, for Hive partition `PARTITION (dt='2008-08-08',
   * country='us')`, its partition name is "dt=2008-08-08/country=us", field names are [["dt"],
   * ["country"]] and values are ["2008-08-08", "us"].
   *
   * @param name The name of the partition.
   * @param fieldNames The field names of the identity partition.
   * @param value The value of the identity partition.
   * @param properties The properties of the partition.
   * @return The created partition.
   */
  public static IdentityPartition identity(
      String name, String[][] fieldNames, Literal<?>[] value, Map<String, String> properties) {
    return new IdentityPartition(name, fieldNames, value, properties);
  }

  /** Represents a result of range partitioning. */
  public static class RangePartition implements Partition {
    private final String name;
    private final Literal<?> upper;
    private final Literal<?> lower;

    private final Map<String, String> properties;

    private RangePartition(
        String name, Literal<?> upper, Literal<?> lower, Map<String, String> properties) {
      this.name = name;
      this.properties = properties;
      this.upper = upper;
      this.lower = lower;
    }

    /** @return The upper bound of the partition. */
    public Literal<?> upper() {
      return upper;
    }

    /** @return The lower bound of the partition. */
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
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RangePartition that = (RangePartition) o;
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
  public static class ListPartition implements Partition {
    private final String name;
    private final Literal<?>[][] lists;

    private final Map<String, String> properties;

    private ListPartition(String name, Literal<?>[][] lists, Map<String, String> properties) {
      this.name = name;
      this.properties = properties;
      this.lists = lists;
    }

    /** @return The values of the list partition. */
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
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ListPartition that = (ListPartition) o;
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
  public static class IdentityPartition implements Partition {
    private final String name;
    private final String[][] fieldNames;
    private final Literal<?>[] value;
    private final Map<String, String> properties;

    private IdentityPartition(
        String name, String[][] fieldNames, Literal<?>[] value, Map<String, String> properties) {
      this.name = name;
      this.fieldNames = fieldNames;
      this.value = value;
      this.properties = properties;
    }

    /** @return The field names of the identity partition. */
    public String[][] fieldNames() {
      return fieldNames;
    }

    /** @return The values of the identity partition. */
    public Literal<?>[] value() {
      return value;
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
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      IdentityPartition that = (IdentityPartition) o;
      return Objects.equals(name, that.name)
          && Arrays.deepEquals(fieldNames, that.fieldNames)
          && Arrays.equals(value, that.value)
          && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(name, properties);
      result = 31 * result + Arrays.deepHashCode(fieldNames);
      result = 31 * result + Arrays.hashCode(value);
      return result;
    }
  }
}
