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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import lombok.EqualsAndHashCode;
import org.apache.gravitino.connector.BaseColumn;

/** Represents a column in an Apache Iceberg column. */
@EqualsAndHashCode(callSuper = true)
public class IcebergColumn extends BaseColumn {

  private IcebergColumn() {}

  /** A builder class for constructing IcebergColumn instances. */
  public static class Builder extends BaseColumnBuilder<Builder, IcebergColumn> {
    /** Creates a new instance of {@link Builder}. */
    private Builder() {}
    /**
     * Internal method to build a IcebergColumn instance using the provided values.
     *
     * @return A new IcebergColumn instance with the configured values.
     */
    @Override
    protected IcebergColumn internalBuild() {
      IcebergColumn icebergColumn = new IcebergColumn();
      icebergColumn.name = name;
      icebergColumn.comment = comment;
      icebergColumn.dataType = dataType;
      icebergColumn.nullable = nullable;
      icebergColumn.defaultValue = defaultValue == null ? DEFAULT_VALUE_NOT_SET : defaultValue;
      icebergColumn.auditInfo = auditInfo;
      return icebergColumn;
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
