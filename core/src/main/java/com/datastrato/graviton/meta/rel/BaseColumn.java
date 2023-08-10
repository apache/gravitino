/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.meta.rel;

import com.datastrato.graviton.Entity;
import com.datastrato.graviton.Field;
import com.datastrato.graviton.rel.Column;
import com.google.common.collect.Maps;
import io.substrait.type.Type;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** An abstract class representing a base column in a relational database. */
@EqualsAndHashCode
@ToString
public abstract class BaseColumn implements Column, Entity {
  public static final Field NAME = Field.required("name", String.class, "The column's name");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment or description for the column");
  public static final Field TYPE =
      Field.required("dataType", Type.class, "The data type of the column");

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
   * Returns the type of the entity, which is {@link EntityType#COLUMN}.
   *
   * @return The type of the entity.
   */
  @Override
  public EntityType type() {
    return EntityType.COLUMN;
  }

  /**
   * Returns a map of the fields and their corresponding values for this column.
   *
   * @return A map of the fields and values.
   */
  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(TYPE, dataType);

    return fields;
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
      t.validate();
      return t;
    }

    private SELF self() {
      return (SELF) this;
    }

    protected abstract T internalBuild();
  }
}
