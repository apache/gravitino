/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

import com.datastrato.graviton.rel.transforms.Transform;
import com.datastrato.graviton.rel.transforms.Transforms;

public class SortOrder {
  /**
   * Sort transform. This is the transform that is used to sort the data. Say we have a table with 3
   * columns: a, b, c. We want to sort on a. Then the sort transform is a. Generally, the sort
   * transform is a field transform.
   */
  private final Transform transform;

  /** Sort direction. Default is ASC. */
  private final Direction direction;

  /** Sort null order. Default is FIRST. */
  private final NullOrdering nullOrdering;

  private SortOrder(Transform transform, Direction direction, NullOrdering nullOrdering) {
    this.transform = transform;
    this.direction = direction;
    this.nullOrdering = nullOrdering;
  }

  /**
   * Creates a name reference sort order instance, i.e., if we want to create a sort order on a
   * column like sort/order by "columnName desc" where 'columnName' is the column name
   *
   * <p>Then we can call this method like:
   *
   * <pre>
   * SQL syntax:  sort/order by columnName desc NULLS FIRST
   * fieldSortOrder("columnName", Direction.DESC, NullOrdering.FIRST)
   * </pre>
   *
   * @param direction direction of the sort order, i.e., ASC or DESC
   * @param nullOrdering null ordering of the sort order, i.e., FIRST or LAST
   * @param fieldName name reference of the sort order, i.e., the name of the field
   */
  public static SortOrder fieldSortOrder(
      String[] fieldName, Direction direction, NullOrdering nullOrdering) {
    return new SortOrder(Transforms.field(fieldName), direction, nullOrdering);
  }

  /**
   * Creates a function-based sort order instance, i.e.,if we want to create a sort order on a
   * column like sort/order by "length(columnName) desc" where 'columnName' is the column name
   *
   * <p>Note: this method is only used for functions that only has one parameter and the parameter
   * is a field reference, like length(columnName), year(columnName), etc. For functions that has
   * more than one parameter, like toDate(ts, 'Asia/Shanghai'), we need to use {@link Builder} to
   * create the sort order instance.
   *
   * <p>Then we can call this method like:
   *
   * <pre>
   * SQL syntax: sort/order by length(columnName) desc NULLS FIRST
   * functionSortOrder("length", "columnName", "Direction.DESC, NullOrdering.FIRST)
   * </pre>
   *
   * <pre>
   * SQL syntax: sort/order by hash(columnName) ASC NULLS LAST
   * functionSortOrder("hash", "columnName", "Direction.DESC, NullOrdering.LAST)
   * </pre>
   *
   * @param direction direction of the sort order, i.e., ASC or DESC
   * @param nullOrdering null ordering of the sort order, i.e., FIRST or LAST
   * @param name name of the function, i.e., length
   * @param fieldName field of the function, i.e., columnName
   */
  public static SortOrder functionSortOrder(
      String name, String[] fieldName, Direction direction, NullOrdering nullOrdering) {
    return new SortOrder(
        Transforms.function(name, new Transform[] {Transforms.field(fieldName)}),
        direction,
        nullOrdering);
  }

  public Transform getTransform() {
    return transform;
  }

  public Direction getDirection() {
    return direction;
  }

  public NullOrdering getNullOrdering() {
    return nullOrdering;
  }

  public enum Direction {
    ASC,
    DESC;

    public static Direction fromString(String value) {
      return Direction.valueOf(value.toUpperCase());
    }
  }

  public enum NullOrdering {
    FIRST,
    LAST;

    public static NullOrdering fromString(String value) {
      return NullOrdering.valueOf(value.toUpperCase());
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Direction direction;
    private NullOrdering nullOrdering;
    private Transform transform;

    public Builder withDirection(Direction direction) {
      this.direction = direction;
      return this;
    }

    public Builder withNullOrdering(NullOrdering nullOrder) {
      this.nullOrdering = nullOrder;
      return this;
    }

    public Builder withTransform(Transform transform) {
      this.transform = transform;
      return this;
    }

    public SortOrder build() {
      if (direction == null) {
        direction = Direction.ASC;
      }
      if (nullOrdering == null) {
        nullOrdering = NullOrdering.FIRST;
      }
      return new SortOrder(transform, direction, nullOrdering);
    }
  }
}
