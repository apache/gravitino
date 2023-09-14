/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.rel;

import com.datastrato.graviton.rel.Column;
import io.substrait.type.Type;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** An abstract class representing a base column in a relational database. */
@ToString
@EqualsAndHashCode
public abstract class BaseColumn implements Column {

  protected String name;

  @Nullable protected String comment;

  protected Type dataType;

  /**
   * Returns the name of the column.
   *
   * @return The name of the column.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * Returns the data type of the column.
   *
   * @return The data type of the column.
   */
  @Override
  public Type dataType() {
    return dataType;
  }

  /**
   * Returns the comment or description for the column.
   *
   * @return The comment or description for the column.
   */
  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  /**
   * Builder interface for creating instances of {@link BaseColumn}.
   *
   * @param <SELF> The type of the builder.
   * @param <T> The type of the column being built.
   */
  interface Builder<SELF extends BaseColumn.Builder<SELF, T>, T extends BaseColumn> {
    SELF withName(String name);

    SELF withComment(String comment);

    SELF withType(Type dataType);

    T build();
  }

  /**
   * An abstract class implementing the builder interface for {@link BaseColumn}.
   *
   * @param <SELF> The type of the builder.
   * @param <T> The type of the column being built.
   */
  public abstract static class BaseColumnBuilder<
          SELF extends BaseColumn.Builder<SELF, T>, T extends BaseColumn>
      implements BaseColumn.Builder<SELF, T> {
    protected String name;
    protected String comment;
    protected Type dataType;

    /**
     * Sets the name of the column.
     *
     * @param name The name of the column.
     * @return The builder instance.
     */
    @Override
    public SELF withName(String name) {
      this.name = name;
      return self();
    }

    /**
     * Sets the comment of the column.
     *
     * @param comment The comment or description for the column.
     * @return The builder instance.
     */
    @Override
    public SELF withComment(String comment) {
      this.comment = comment;
      return self();
    }

    /**
     * Sets the data type of the column.
     *
     * @param dataType The data type of the column.
     * @return The builder instance.
     */
    @Override
    public SELF withType(Type dataType) {
      this.dataType = dataType;
      return self();
    }

    /**
     * Builds the instance of the column with the provided attributes.
     *
     * @return The built column instance.
     */
    @Override
    public T build() {
      T t = internalBuild();
      return t;
    }

    private SELF self() {
      return (SELF) this;
    }

    protected abstract T internalBuild();
  }
}
