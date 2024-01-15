/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc;

import com.datastrato.gravitino.catalog.rel.BaseColumn;
import java.util.List;

/** Represents a column in the Jdbc column. */
public class JdbcColumn extends BaseColumn {
  private List<String> properties;

  private JdbcColumn() {}

  public List<String> getProperties() {
    return properties;
  }

  /** A builder class for constructing JdbcColumn instances. */
  public static class Builder extends BaseColumnBuilder<Builder, JdbcColumn> {

    /** Attribute value of the field, such as AUTO_INCREMENT, PRIMARY KEY, etc. */
    private List<String> properties;

    public Builder withProperties(List<String> properties) {
      this.properties = properties;
      return this;
    }

    /**
     * Internal method to build a JdbcColumn instance using the provided values.
     *
     * @return A new JdbcColumn instance with the configured values.
     */
    @Override
    protected JdbcColumn internalBuild() {
      JdbcColumn jdbcColumn = new JdbcColumn();
      jdbcColumn.name = name;
      jdbcColumn.comment = comment;
      jdbcColumn.dataType = dataType;
      jdbcColumn.nullable = nullable;
      jdbcColumn.defaultValue = defaultValue;
      jdbcColumn.properties = properties;
      jdbcColumn.autoIncrement = autoIncrement;
      return jdbcColumn;
    }
  }
}
