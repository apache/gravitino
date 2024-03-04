/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc;

import com.datastrato.gravitino.catalog.rel.BaseColumn;
import lombok.EqualsAndHashCode;

/** Represents a column in the Jdbc column. */
@EqualsAndHashCode(callSuper = true)
public class JdbcColumn extends BaseColumn {

  private JdbcColumn() {}

  /** A builder class for constructing JdbcColumn instances. */
  public static class Builder extends BaseColumnBuilder<Builder, JdbcColumn> {

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
      // In theory, defaultValue should never be null, because we set it to
      // DEFAULT_VALUE_NOT_SET if it is null in JSONSerde. But in case of the JdbcColumn is created
      // by other ways(e.g. integration test), we still need to handle the null case.
      jdbcColumn.defaultValue = defaultValue == null ? DEFAULT_VALUE_NOT_SET : defaultValue;
      jdbcColumn.autoIncrement = autoIncrement;
      return jdbcColumn;
    }
  }
}
