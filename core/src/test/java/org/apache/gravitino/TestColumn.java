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

  private int position;

  private TestColumn() {}

  public int position() {
    return position;
  }

  public void setPosition(int position) {
    this.position = position;
  }

  public static class Builder extends BaseColumn.BaseColumnBuilder<Builder, TestColumn> {

    private Integer position;

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    public Builder withPosition(int position) {
      this.position = position;
      return this;
    }

    @Override
    protected TestColumn internalBuild() {
      TestColumn column = new TestColumn();

      if (position == null) {
        throw new IllegalArgumentException("Position is required");
      }

      column.name = name;
      column.position = position;
      column.comment = comment;
      column.dataType = dataType;
      column.nullable = nullable;
      column.autoIncrement = autoIncrement;
      column.defaultValue = defaultValue;
      column.auditInfo = auditInfo;

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
