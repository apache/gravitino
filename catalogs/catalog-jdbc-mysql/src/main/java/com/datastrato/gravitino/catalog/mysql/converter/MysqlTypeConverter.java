/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql.converter;

import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import java.util.Objects;

/** Type converter for MySQL. */
public class MysqlTypeConverter
    extends JdbcTypeConverter<MysqlTypeConverter.MysqlTypeBean, String> {

  @Override
  public Type toGravitinoType(MysqlTypeBean typeBean) {
    switch (typeBean.getTypeName().toLowerCase()) {
      case "tinyint":
        return Types.ByteType.get();
      case "smallint":
        return Types.ShortType.get();
      case "int":
        return Types.IntegerType.get();
      case "bigint":
        return Types.LongType.get();
      case "float":
        return Types.FloatType.get();
      case "double":
        return Types.DoubleType.get();
      case "date":
        return Types.DateType.get();
      case "time":
        return Types.TimeType.get();
      case "timestamp":
        return Types.TimestampType.withoutTimeZone();
      case "decimal":
        return Types.DecimalType.of(
            Integer.parseInt(typeBean.getColumnSize()), Integer.parseInt(typeBean.getScale()));
      case "varchar":
        return Types.VarCharType.of(Integer.parseInt(typeBean.getColumnSize()));
      case "char":
        return Types.FixedCharType.of(Integer.parseInt(typeBean.getColumnSize()));
      case "text":
        return Types.StringType.get();
      case "binary":
        return Types.BinaryType.get();
      default:
        throw new IllegalArgumentException("Not a supported type: " + typeBean.getTypeName());
    }
  }

  @Override
  public String fromGravitinoType(Type type) {
    if (type instanceof Types.ByteType) {
      return "tinyint";
    } else if (type instanceof Types.ShortType) {
      return "smallint";
    } else if (type instanceof Types.IntegerType) {
      return "int";
    } else if (type instanceof Types.LongType) {
      return "bigint";
    } else if (type instanceof Types.FloatType) {
      return type.simpleString();
    } else if (type instanceof Types.DoubleType) {
      return type.simpleString();
    } else if (type instanceof Types.StringType) {
      return "text";
    } else if (type instanceof Types.DateType) {
      return type.simpleString();
    } else if (type instanceof Types.TimeType) {
      return type.simpleString();
    } else if (type instanceof Types.TimestampType && !((Types.TimestampType) type).hasTimeZone()) {
      return type.simpleString();
    } else if (type instanceof Types.DecimalType) {
      return type.simpleString();
    } else if (type instanceof Types.VarCharType) {
      return type.simpleString();
    } else if (type instanceof Types.FixedCharType) {
      return type.simpleString();
    } else if (type instanceof Types.BinaryType) {
      return type.simpleString();
    }
    throw new IllegalArgumentException("Not a supported type: " + type.toString());
  }

  public static class MysqlTypeBean {
    private String typeName;
    private String columnSize;
    private String scale;

    public MysqlTypeBean(String typeName) {
      this.typeName = typeName;
    }

    public String getTypeName() {
      return typeName;
    }

    public void setTypeName(String typeName) {
      this.typeName = typeName;
    }

    public String getColumnSize() {
      return columnSize;
    }

    public void setColumnSize(String columnSize) {
      this.columnSize = columnSize;
    }

    public String getScale() {
      return scale;
    }

    public void setScale(String scale) {
      this.scale = scale;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      MysqlTypeBean typeBean = (MysqlTypeBean) o;
      return Objects.equals(typeName, typeBean.typeName)
          && Objects.equals(columnSize, typeBean.columnSize)
          && Objects.equals(scale, typeBean.scale);
    }

    @Override
    public int hashCode() {
      return Objects.hash(typeName, columnSize, scale);
    }
  }
}
