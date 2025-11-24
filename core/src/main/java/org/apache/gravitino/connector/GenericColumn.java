/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.connector;

import org.apache.gravitino.annotation.DeveloperApi;

/**
 * A generic implementation of a table column. This is used to represent column metadata that is
 * managed by Gravitino.
 */
@DeveloperApi
public class GenericColumn extends BaseColumn {

  /**
   * Creates a new builder for constructing a GenericColumn instance.
   *
   * @return A new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** A builder class for constructing GenericColumn instances. */
  public static class Builder extends BaseColumnBuilder<Builder, GenericColumn> {

    /**
     * Internal method to build a GenericColumn instance using the provided values.
     *
     * @return A new GenericColumn instance with the configured values.
     */
    @Override
    protected GenericColumn internalBuild() {
      GenericColumn column = new GenericColumn();

      column.name = name;
      column.comment = comment;
      column.dataType = dataType;
      column.nullable = nullable;
      column.defaultValue = defaultValue == null ? DEFAULT_VALUE_NOT_SET : defaultValue;
      column.autoIncrement = autoIncrement;
      return column;
    }
  }
}
