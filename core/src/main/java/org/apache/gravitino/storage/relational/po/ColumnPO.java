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
package org.apache.gravitino.storage.relational.po;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@EqualsAndHashCode
@Getter
public class ColumnPO {

  public enum ColumnOpType {
    CREATE((byte) 1),
    UPDATE((byte) 2),
    DELETE((byte) 3);

    private final byte value;

    ColumnOpType(byte value) {
      this.value = value;
    }

    public byte value() {
      return value;
    }
  }

  public enum Nullable {
    TRUE((byte) 0, true),
    FALSE((byte) 1, false);

    private final byte value;

    private final boolean nullable;

    Nullable(byte value, boolean nullable) {
      this.value = value;
      this.nullable = nullable;
    }

    public Byte value() {
      return value;
    }

    public boolean nullable() {
      return nullable;
    }

    public static Nullable fromValue(byte value) {
      for (Nullable nullable : values()) {
        if (nullable.value == value) {
          return nullable;
        }
      }
      throw new IllegalArgumentException("Invalid nullable value: " + value);
    }

    public static Nullable fromBoolean(boolean nullable) {
      for (Nullable nullableEnum : values()) {
        if (nullableEnum.nullable == nullable) {
          return nullableEnum;
        }
      }
      throw new IllegalArgumentException("Invalid nullable boolean value: " + nullable);
    }
  }

  public enum AutoIncrement {
    TRUE((byte) 0, true),
    FALSE((byte) 1, false);

    private final byte value;

    private final boolean autoIncrement;

    AutoIncrement(Byte value, boolean autoIncrement) {
      this.value = value;
      this.autoIncrement = autoIncrement;
    }

    public Byte value() {
      return value;
    }

    public boolean autoIncrement() {
      return autoIncrement;
    }

    public static AutoIncrement fromValue(byte value) {
      for (AutoIncrement autoIncrement : values()) {
        if (autoIncrement.value == value) {
          return autoIncrement;
        }
      }
      throw new IllegalArgumentException("Invalid auto increment value: " + value);
    }

    public static AutoIncrement fromBoolean(boolean autoIncrement) {
      for (AutoIncrement autoIncrementEnum : values()) {
        if (autoIncrementEnum.autoIncrement == autoIncrement) {
          return autoIncrementEnum;
        }
      }
      throw new IllegalArgumentException("Invalid auto increment boolean value: " + autoIncrement);
    }
  }

  private Long columnId;

  private String columnName;

  private Integer columnPosition;

  private Long metalakeId;

  private Long catalogId;

  private Long schemaId;

  private Long tableId;

  private Long tableVersion;

  private String columnType;

  private String columnComment;

  private Byte nullable;

  private Byte autoIncrement;

  private String defaultValue;

  private Byte columnOpType;

  private Long deletedAt;

  private String auditInfo;

  public static Builder builder() {
    return new Builder();
  }

  private ColumnPO() {}

  public static class Builder {

    private final ColumnPO columnPO;

    private Builder() {
      columnPO = new ColumnPO();
    }

    public Builder withColumnId(Long columnId) {
      columnPO.columnId = columnId;
      return this;
    }

    public Builder withColumnName(String columnName) {
      columnPO.columnName = columnName;
      return this;
    }

    public Builder withColumnPosition(Integer columnPosition) {
      columnPO.columnPosition = columnPosition;
      return this;
    }

    public Builder withMetalakeId(Long metalakeId) {
      columnPO.metalakeId = metalakeId;
      return this;
    }

    public Builder withCatalogId(Long catalogId) {
      columnPO.catalogId = catalogId;
      return this;
    }

    public Builder withSchemaId(Long schemaId) {
      columnPO.schemaId = schemaId;
      return this;
    }

    public Builder withTableId(Long tableId) {
      columnPO.tableId = tableId;
      return this;
    }

    public Builder withTableVersion(Long tableVersion) {
      columnPO.tableVersion = tableVersion;
      return this;
    }

    public Builder withColumnType(String columnType) {
      columnPO.columnType = columnType;
      return this;
    }

    public Builder withColumnComment(String columnComment) {
      columnPO.columnComment = columnComment;
      return this;
    }

    public Builder withNullable(Byte nullable) {
      columnPO.nullable = nullable;
      return this;
    }

    public Builder withAutoIncrement(Byte autoIncrement) {
      columnPO.autoIncrement = autoIncrement;
      return this;
    }

    public Builder withDefaultValue(String defaultValue) {
      columnPO.defaultValue = defaultValue;
      return this;
    }

    public Builder withColumnOpType(Byte columnOpType) {
      columnPO.columnOpType = columnOpType;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      columnPO.deletedAt = deletedAt;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      columnPO.auditInfo = auditInfo;
      return this;
    }

    public ColumnPO build() {
      Preconditions.checkArgument(columnPO.columnId != null, "Column id is required");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(columnPO.columnName),
          "Column name is required and cannot be blank");
      Preconditions.checkArgument(columnPO.columnPosition != null, "Column position is required");
      Preconditions.checkArgument(columnPO.metalakeId != null, "Metalake id is required");
      Preconditions.checkArgument(columnPO.catalogId != null, "Catalog id is required");
      Preconditions.checkArgument(columnPO.schemaId != null, "Schema id is required");
      Preconditions.checkArgument(columnPO.tableId != null, "Table id is required");
      Preconditions.checkArgument(columnPO.tableVersion != null, "Table version is required");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(columnPO.columnType),
          "Column type is required and cannot be blank");
      Preconditions.checkArgument(columnPO.nullable != null, "Nullable is required");
      Preconditions.checkArgument(columnPO.autoIncrement != null, "Auto increment is required");
      Preconditions.checkArgument(
          columnPO.columnOpType != null, "Column operation type is required");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(columnPO.auditInfo), "Audit info is required and cannot be blank");
      Preconditions.checkArgument(columnPO.deletedAt != null, "Deleted at is required");

      return columnPO;
    }
  }
}
