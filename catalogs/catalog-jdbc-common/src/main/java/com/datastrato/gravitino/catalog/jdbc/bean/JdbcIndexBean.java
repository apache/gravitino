/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.bean;

import com.datastrato.gravitino.rel.indexes.Index;
import java.util.Objects;

/** Store JDBC index information. */
public class JdbcIndexBean {

  private final Index.IndexType indexType;

  private final String colName;

  private final String name;

  /** Used for sorting */
  private final int order;

  public JdbcIndexBean(Index.IndexType indexType, String colName, String name, int order) {
    this.indexType = indexType;
    this.colName = colName;
    this.name = name;
    this.order = order;
  }

  public Index.IndexType getIndexType() {
    return indexType;
  }

  public String getColName() {
    return colName;
  }

  public String getName() {
    return name;
  }

  public int getOrder() {
    return order;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof JdbcIndexBean)) return false;
    JdbcIndexBean that = (JdbcIndexBean) o;
    return order == that.order
        && indexType == that.indexType
        && Objects.equals(colName, that.colName)
        && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(indexType, colName, name, order);
  }

  @Override
  public String toString() {
    return "JdbcIndexBean{"
        + "indexType="
        + indexType
        + ", colName='"
        + colName
        + '\''
        + ", name='"
        + name
        + '\''
        + ", order="
        + order
        + '}';
  }
}
