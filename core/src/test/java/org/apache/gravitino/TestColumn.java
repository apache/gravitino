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
package org.apache.gravitino;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.gravitino.connector.BaseColumn;

@EqualsAndHashCode(callSuper = true)
@ToString
public class TestColumn extends BaseColumn {

  private TestColumn() {}

  public static class Builder extends BaseColumn.BaseColumnBuilder<Builder, TestColumn> {
    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    @Override
    protected TestColumn internalBuild() {
      TestColumn column = new TestColumn();

      column.name = name;
      column.comment = comment;
      column.dataType = dataType;
      column.nullable = nullable;
      column.defaultValue = defaultValue;

      return column;
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
