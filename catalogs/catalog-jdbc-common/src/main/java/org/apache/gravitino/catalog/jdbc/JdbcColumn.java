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
package org.apache.gravitino.catalog.jdbc;

import lombok.EqualsAndHashCode;
import org.apache.gravitino.connector.BaseColumn;

/** Represents a column in the Jdbc column. */
@EqualsAndHashCode(callSuper = true)
public class JdbcColumn extends BaseColumn {

  private JdbcColumn() {}

  /** A builder class for constructing JdbcColumn instances. */
  public static class Builder extends BaseColumnBuilder<Builder, JdbcColumn> {
    /** Creates a new instance of {@link Builder}. */
    private Builder() {}
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
      jdbcColumn.auditInfo = auditInfo;
      return jdbcColumn;
    }
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}
