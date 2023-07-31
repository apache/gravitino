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

@EqualsAndHashCode
@ToString
public abstract class BaseColumn implements Column, Entity {
  public static final Field NAME = Field.required("name", String.class, "The name of the column");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the column");
  public static final Field TYPE = Field.required("dataType", Type.class, "The type of the column");

  protected String name;

  @Nullable protected String comment;

  protected Type dataType;

  @Override
  public String name() {
    return name;
  }

  @Override
  public Type dataType() {
    return dataType;
  }

  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  @Override
  public EntityType type() {
    return EntityType.COLUMN;
  }

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(TYPE, dataType);

    return fields;
  }

  interface Builder<SELF extends BaseColumn.Builder<SELF, T>, T extends BaseColumn> {
    SELF withName(String name);

    SELF withComment(String comment);

    SELF withType(Type dataType);

    T build();
  }

  public abstract static class BaseColumnBuilder<
          SELF extends BaseColumn.Builder<SELF, T>, T extends BaseColumn>
      implements BaseColumn.Builder<SELF, T> {
    protected String name;
    protected String comment;
    protected Type dataType;

    @Override
    public SELF withName(String name) {
      this.name = name;
      return self();
    }

    @Override
    public SELF withComment(String comment) {
      this.comment = comment;
      return self();
    }

    @Override
    public SELF withType(Type dataType) {
      this.dataType = dataType;
      return self();
    }

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
