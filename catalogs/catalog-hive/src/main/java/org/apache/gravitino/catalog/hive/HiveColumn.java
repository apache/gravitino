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
package org.apache.gravitino.catalog.hive;

import lombok.EqualsAndHashCode;
import org.apache.gravitino.connector.BaseColumn;

/** Represents a column in an Apache Hive Metastore catalog. */
@EqualsAndHashCode(callSuper = true)
public class HiveColumn extends BaseColumn {

  private HiveColumn() {}

  /** A builder class for constructing HiveColumn instances. */
  public static class Builder extends BaseColumnBuilder<Builder, HiveColumn> {

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    /**
     * Internal method to build a HiveColumn instance using the provided values.
     *
     * @return A new HiveColumn instance with the configured values.
     */
    @Override
    protected HiveColumn internalBuild() {
      HiveColumn hiveColumn = new HiveColumn();

      hiveColumn.name = name;
      hiveColumn.comment = comment;
      hiveColumn.dataType = dataType;
      hiveColumn.nullable = nullable;
      hiveColumn.defaultValue = defaultValue == null ? DEFAULT_VALUE_NOT_SET : defaultValue;
      hiveColumn.auditInfo = auditInfo;
      return hiveColumn;
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
