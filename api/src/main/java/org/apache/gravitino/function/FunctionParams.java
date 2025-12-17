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
import com.google.common.base.Strings;
import java.util.Objects;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.types.Type;

/** Helper methods to create {@link FunctionParam} instances. */
public class FunctionParams {

  private FunctionParams() {}

  /**
   * Create a {@link FunctionParam} instance.
   *
   * @param name The parameter name.
   * @param dataType The parameter type.
   * @return A {@link FunctionParam} instance.
   */
  public static FunctionParam of(String name, Type dataType) {
    return of(name, dataType, null, Column.DEFAULT_VALUE_NOT_SET);
  }

  /**
   * Create a {@link FunctionParam} instance with an optional comment.
   *
   * @param name The parameter name.
   * @param dataType The parameter type.
   * @param comment The optional comment.
   * @return A {@link FunctionParam} instance.
   */
  public static FunctionParam of(String name, Type dataType, String comment) {
    return of(name, dataType, comment, Column.DEFAULT_VALUE_NOT_SET);
  }

  /**
   * Create a {@link FunctionParam} instance with an optional comment and default value.
   *
   * @param name The parameter name.
   * @param dataType The parameter type.
   * @param comment The optional comment.
   * @param defaultValue The optional default value expression.
   * @return A {@link FunctionParam} instance.
   */
  public static FunctionParam of(
      String name, Type dataType, String comment, Expression defaultValue) {
    return new FunctionParamImpl(name, dataType, comment, defaultValue);
  }

  private static final class FunctionParamImpl implements FunctionParam {
    private final String name;
    private final Type dataType;
    private final String comment;
    private final Expression defaultValue;

    private FunctionParamImpl(String name, Type dataType, String comment, Expression defaultValue) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Parameter name cannot be null");
      this.name = name;
      this.dataType = Preconditions.checkNotNull(dataType, "Parameter data type cannot be null");
      this.comment = comment;
      this.defaultValue = defaultValue == null ? Column.DEFAULT_VALUE_NOT_SET : defaultValue;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Type dataType() {
      return dataType;
    }

    @Override
    public String comment() {
      return comment;
    }

    @Override
    public Expression defaultValue() {
      return defaultValue;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof FunctionParamImpl)) {
        return false;
      }
      FunctionParamImpl that = (FunctionParamImpl) obj;
      return Objects.equals(name, that.name)
          && Objects.equals(dataType, that.dataType)
          && Objects.equals(comment, that.comment)
          && Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, dataType, comment, defaultValue);
    }

    @Override
    public String toString() {
      return "FunctionParam{"
          + "name='"
          + name
          + '\''
          + ", dataType="
          + dataType
          + ", comment='"
          + comment
          + '\''
          + ", defaultValue="
          + defaultValue
          + '}';
    }
  }
}
