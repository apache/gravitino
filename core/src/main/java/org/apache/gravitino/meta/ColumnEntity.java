/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.meta;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import lombok.ToString;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.types.Type;

/**
 * A class representing a column entity in Apache Gravitino. Columns belong to table, it uses table
 * name identifier and column name as identifier.
 */
@ToString
public class ColumnEntity implements Entity, Auditable {

  public static final Field ID = Field.required("id", Long.class, "The column's unique identifier");
  public static final Field NAME = Field.required("name", String.class, "The column's name");
  public static final Field POSITION =
      Field.required("position", Integer.class, "The column's position");
  public static final Field TYPE = Field.required("dataType", Type.class, "The column's data type");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The column's comment");
  public static final Field NULLABLE =
      Field.required("nullable", Boolean.class, "The column's nullable property");
  public static final Field AUTO_INCREMENT =
      Field.required("auto_increment", Boolean.class, "The column's auto increment property");
  public static final Field DEFAULT_VALUE =
      Field.optional("default_value", Expression.class, "The column's default value");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", Audit.class, "The column's audit information");

  private Long id;

  private String name;

  private Integer position;

  private Type dataType;

  private String comment;

  private boolean nullable;

  private boolean autoIncrement;

  private Expression defaultValue;

  private AuditInfo auditInfo;

  private ColumnEntity() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(POSITION, position);
    fields.put(TYPE, dataType);
    fields.put(COMMENT, comment);
    fields.put(NULLABLE, nullable);
    fields.put(AUTO_INCREMENT, autoIncrement);
    fields.put(DEFAULT_VALUE, defaultValue);
    fields.put(AUDIT_INFO, auditInfo);

    return Collections.unmodifiableMap(fields);
  }

  @Override
  public EntityType type() {
    return EntityType.COLUMN;
  }

  @Override
  public Audit auditInfo() {
    return auditInfo;
  }

  public Long id() {
    return id;
  }

  public String name() {
    return name;
  }

  public Integer position() {
    return position;
  }

  public Type dataType() {
    return dataType;
  }

  public String comment() {
    return comment;
  }

  public boolean nullable() {
    return nullable;
  }

  public boolean autoIncrement() {
    return autoIncrement;
  }

  public Expression defaultValue() {
    return defaultValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ColumnEntity)) return false;

    ColumnEntity that = (ColumnEntity) o;
    return Objects.equal(id, that.id)
        && Objects.equal(name, that.name)
        && Objects.equal(position, that.position)
        && Objects.equal(dataType, that.dataType)
        && Objects.equal(comment, that.comment)
        && Objects.equal(nullable, that.nullable)
        && Objects.equal(autoIncrement, that.autoIncrement)
        && Objects.equal(defaultValue, that.defaultValue)
        && Objects.equal(auditInfo, that.auditInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        id, name, position, dataType, comment, nullable, autoIncrement, defaultValue, auditInfo);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static ColumnEntity toColumnEntity(
      Column column, int position, long uid, AuditInfo audit) {
    return builder()
        .withId(uid)
        .withName(column.name())
        .withPosition(position)
        .withComment(column.comment())
        .withDataType(column.dataType())
        .withNullable(column.nullable())
        .withAutoIncrement(column.autoIncrement())
        .withDefaultValue(column.defaultValue())
        .withAuditInfo(audit)
        .build();
  }

  public static class Builder {
    private final ColumnEntity columnEntity;

    public Builder() {
      columnEntity = new ColumnEntity();
    }

    public Builder withId(Long id) {
      columnEntity.id = id;
      return this;
    }

    public Builder withName(String name) {
      columnEntity.name = name;
      return this;
    }

    public Builder withPosition(Integer position) {
      columnEntity.position = position;
      return this;
    }

    public Builder withDataType(Type dataType) {
      columnEntity.dataType = dataType;
      return this;
    }

    public Builder withComment(String comment) {
      columnEntity.comment = comment;
      return this;
    }

    public Builder withNullable(boolean nullable) {
      columnEntity.nullable = nullable;
      return this;
    }

    public Builder withAutoIncrement(boolean autoIncrement) {
      columnEntity.autoIncrement = autoIncrement;
      return this;
    }

    public Builder withDefaultValue(Expression defaultValue) {
      columnEntity.defaultValue = defaultValue;
      return this;
    }

    public Builder withAuditInfo(AuditInfo auditInfo) {
      columnEntity.auditInfo = auditInfo;
      return this;
    }

    public ColumnEntity build() {
      columnEntity.validate();

      if (columnEntity.defaultValue == null) {
        columnEntity.defaultValue = Column.DEFAULT_VALUE_NOT_SET;
      }

      return columnEntity;
    }
  }
}
