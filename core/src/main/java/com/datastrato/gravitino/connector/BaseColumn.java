/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.connector;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.types.Type;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** An abstract class representing a base column in a relational database. */
@Evolving
@ToString
@EqualsAndHashCode
public abstract class BaseColumn implements Column {

  protected String name;

  @Nullable protected String comment;

  protected Type dataType;

  protected boolean nullable;

  protected boolean autoIncrement;

  protected Expression defaultValue;

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

  /** Returns true if the column value can be null. */
  @Override
  public boolean nullable() {
    return nullable;
  }

  /** @return True if this column is an auto-increment column. Default is false. */
  @Override
  public boolean autoIncrement() {
    return autoIncrement;
  }

  /**
   * @return The default value of this column, {@link Column#DEFAULT_VALUE_NOT_SET} if not specified
   */
  @Override
  public Expression defaultValue() {
    return defaultValue;
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

    SELF withNullable(boolean nullable);

    SELF withAutoIncrement(boolean autoIncrement);

    SELF withDefaultValue(Expression defaultValue);

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
    protected boolean nullable = true;
    protected boolean autoIncrement = false;
    protected Expression defaultValue;

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
     * Sets nullable of the column value.
     *
     * @param nullable Nullable of the column.
     * @return The builder instance.
     */
    @Override
    public SELF withNullable(boolean nullable) {
      this.nullable = nullable;
      return self();
    }

    /**
     * Sets whether the column is an auto-increment column.
     *
     * @param autoIncrement Whether the column is an auto-increment column.
     * @return The builder instance.
     */
    @Override
    public SELF withAutoIncrement(boolean autoIncrement) {
      this.autoIncrement = autoIncrement;
      return self();
    }

    /**
     * Sets the default value of the column.
     *
     * @param defaultValue The default value of the column.
     * @return The builder instance.
     */
    @Override
    public SELF withDefaultValue(Expression defaultValue) {
      this.defaultValue = defaultValue;
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

    /**
     * Builds the concrete instance of the column with the provided attributes.
     *
     * @return The built column instance.
     */
    @Evolving
    protected abstract T internalBuild();
  }
}
