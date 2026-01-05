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
package org.apache.gravitino.function;

import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.rel.types.Type;

/** Represents a return column of a table-valued function. */
@Evolving
public class FunctionColumn {
  private final String name;
  private final Type dataType;
  private final String comment;

  private FunctionColumn(String name, Type dataType, String comment) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "Function column name cannot be null or empty");
    this.name = name;
    Preconditions.checkArgument(dataType != null, "Function column type cannot be null");
    this.dataType = dataType;
    this.comment = comment;
  }

  /**
   * Create a {@link FunctionColumn} instance.
   *
   * @param name The column name.
   * @param dataType The column type.
   * @param comment The optional comment of the column.
   * @return A {@link FunctionColumn} instance.
   */
  public static FunctionColumn of(String name, Type dataType, String comment) {
    return new FunctionColumn(name, dataType, comment);
  }

  /**
   * @return The column name.
   */
  public String name() {
    return name;
  }

  /**
   * @return The column type.
   */
  public Type dataType() {
    return dataType;
  }

  /**
   * @return The optional column comment, null if not provided.
   */
  @Nullable
  public String comment() {
    return comment;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof FunctionColumn)) {
      return false;
    }
    FunctionColumn that = (FunctionColumn) obj;
    return Objects.equals(name, that.name)
        && Objects.equals(dataType, that.dataType)
        && Objects.equals(comment, that.comment);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, dataType, comment);
  }

  @Override
  public String toString() {
    return "FunctionColumn{"
        + "name='"
        + name
        + '\''
        + ", dataType="
        + dataType
        + ", comment='"
        + comment
        + '\''
        + '}';
  }
}
