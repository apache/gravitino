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
package org.apache.gravitino.catalog.lakehouse.hudi;

import lombok.EqualsAndHashCode;
import org.apache.gravitino.connector.BaseColumn;

/** A class representing a column in a Hudi table. */
@EqualsAndHashCode(callSuper = true)
public class HudiColumn extends BaseColumn {
  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  private HudiColumn() {}

  /** A builder class for constructing HudiColumn instances. */
  public static class Builder extends BaseColumnBuilder<Builder, HudiColumn> {

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    /**
     * Internal method to build a HudiColumn instance using the provided values.
     *
     * @return A new HudiColumn instance with the configured values.
     */
    @Override
    protected HudiColumn internalBuild() {
      HudiColumn hudiColumn = new HudiColumn();

      hudiColumn.name = name;
      hudiColumn.comment = comment;
      hudiColumn.dataType = dataType;
      hudiColumn.nullable = nullable;
      hudiColumn.defaultValue = defaultValue == null ? DEFAULT_VALUE_NOT_SET : defaultValue;
      hudiColumn.auditInfo = auditInfo;
      return hudiColumn;
    }
  }
}
