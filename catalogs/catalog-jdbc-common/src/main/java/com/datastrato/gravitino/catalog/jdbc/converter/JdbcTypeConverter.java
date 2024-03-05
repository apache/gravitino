/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.converter;

import com.datastrato.gravitino.rel.types.Type;
import java.util.Objects;

/** @param <TO> Implement the corresponding JDBC data type to be converted */
public abstract class JdbcTypeConverter<TO> {

  public static final String DATE = "date";
  public static final String TIME = "time";
  public static final String TIMESTAMP = "timestamp";
  public static final String VARCHAR = "varchar";
  public static final String TEXT = "text";

  /**
   * Convert from JDBC type to Gravitino type
   *
   * @param type The common jdbc type bean.
   * @return Gravitino type.
   */
  public abstract Type toGravitinoType(JdbcTypeBean type);

  /**
   * Convert from Gravitino type to JDBC type
   *
   * @param type Gravitino type.
   * @return Implement the corresponding JDBC data type to be converted.
   */
  public abstract TO fromGravitinoType(Type type);

  public static class JdbcTypeBean {
    /** Data type name. */
    private String typeName;

    /** Column size. For example: 20 in varchar (20) and 10 in decimal (10,2). */
    private String columnSize;

    /** Scale. For example: 2 in decimal (10,2). */
    private String scale;

    public JdbcTypeBean(String typeName) {
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
      if (!(o instanceof JdbcTypeBean)) return false;
      JdbcTypeBean typeBean = (JdbcTypeBean) o;
      return Objects.equals(typeName, typeBean.typeName)
          && Objects.equals(columnSize, typeBean.columnSize)
          && Objects.equals(scale, typeBean.scale);
    }

    @Override
    public int hashCode() {
      return Objects.hash(typeName, columnSize, scale);
    }

    @Override
    public String toString() {
      return "JdbcTypeBean{"
          + "typeName='"
          + typeName
          + '\''
          + ", columnSize='"
          + columnSize
          + '\''
          + ", scale='"
          + scale
          + '\''
          + '}';
    }
  }
}
