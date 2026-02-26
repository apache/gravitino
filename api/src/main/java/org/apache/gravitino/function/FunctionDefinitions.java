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
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.rel.types.Type;

/** Helper methods to create {@link FunctionDefinition} instances. */
public final class FunctionDefinitions {

  private FunctionDefinitions() {}

  /**
   * Create an array of {@link FunctionDefinition} instances.
   *
   * @param definitions The function definitions.
   * @return An array of {@link FunctionDefinition} instances.
   */
  public static FunctionDefinition[] of(FunctionDefinition... definitions) {
    Preconditions.checkArgument(
        ArrayUtils.isNotEmpty(definitions), "Definitions cannot be null or empty");
    return Arrays.copyOf(definitions, definitions.length);
  }

  /**
   * Create a {@link FunctionDefinition} instance for a scalar or aggregate function.
   *
   * @param parameters The parameters for this definition, it may be null or empty.
   * @param returnType The return type for this definition, it must not be null.
   * @param impls The implementations for this definition, it must not be null or empty.
   * @return A {@link FunctionDefinition} instance.
   */
  public static FunctionDefinition of(
      FunctionParam[] parameters, Type returnType, FunctionImpl[] impls) {
    Preconditions.checkArgument(returnType != null, "Return type cannot be null");
    return new FunctionDefinitionImpl(parameters, returnType, null, impls);
  }

  /**
   * Create a {@link FunctionDefinition} instance for a table-valued function.
   *
   * @param parameters The parameters for this definition, it may be null or empty.
   * @param returnColumns The return columns for this definition, it must not be null or empty.
   * @param impls The implementations for this definition, it must not be null or empty.
   * @return A {@link FunctionDefinition} instance.
   */
  public static FunctionDefinition of(
      FunctionParam[] parameters, FunctionColumn[] returnColumns, FunctionImpl[] impls) {
    Preconditions.checkArgument(
        ArrayUtils.isNotEmpty(returnColumns), "Return columns cannot be null or empty");
    return new FunctionDefinitionImpl(parameters, null, returnColumns, impls);
  }

  /**
   * Create a {@link FunctionDefinition} instance. This method is kept for backward compatibility.
   *
   * @param parameters The parameters for this definition, it may be null or empty.
   * @param impls The implementations for this definition, it must not be null or empty.
   * @return A {@link FunctionDefinition} instance.
   * @deprecated Use {@link #of(FunctionParam[], Type, FunctionImpl[])} or {@link
   *     #of(FunctionParam[], FunctionColumn[], FunctionImpl[])} instead.
   */
  @Deprecated
  public static FunctionDefinition of(FunctionParam[] parameters, FunctionImpl[] impls) {
    return new FunctionDefinitionImpl(parameters, null, null, impls);
  }

  private static final class FunctionDefinitionImpl implements FunctionDefinition {
    private final FunctionParam[] parameters;
    private final Type returnType;
    private final FunctionColumn[] returnColumns;
    private final FunctionImpl[] impls;

    FunctionDefinitionImpl(
        FunctionParam[] parameters,
        @Nullable Type returnType,
        @Nullable FunctionColumn[] returnColumns,
        FunctionImpl[] impls) {
      this.parameters =
          parameters == null ? new FunctionParam[0] : Arrays.copyOf(parameters, parameters.length);
      this.returnType = returnType;
      this.returnColumns =
          returnColumns == null
              ? FunctionDefinition.EMPTY_COLUMNS
              : Arrays.copyOf(returnColumns, returnColumns.length);
      Preconditions.checkArgument(
          impls != null && impls.length > 0, "Impls cannot be null or empty");
      this.impls = Arrays.copyOf(impls, impls.length);
    }

    @Override
    public FunctionParam[] parameters() {
      return Arrays.copyOf(parameters, parameters.length);
    }

    @Override
    public Type returnType() {
      return returnType;
    }

    @Override
    public FunctionColumn[] returnColumns() {
      return returnColumns.length == 0
          ? FunctionDefinition.EMPTY_COLUMNS
          : Arrays.copyOf(returnColumns, returnColumns.length);
    }

    @Override
    public FunctionImpl[] impls() {
      return Arrays.copyOf(impls, impls.length);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof FunctionDefinition)) {
        return false;
      }
      FunctionDefinition that = (FunctionDefinition) obj;
      return Arrays.equals(parameters, that.parameters())
          && Objects.equals(returnType, that.returnType())
          && Arrays.equals(returnColumns, that.returnColumns())
          && Arrays.equals(impls, that.impls());
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(parameters);
      result = 31 * result + Objects.hashCode(returnType);
      result = 31 * result + Arrays.hashCode(returnColumns);
      result = 31 * result + Arrays.hashCode(impls);
      return result;
    }

    @Override
    public String toString() {
      return "FunctionDefinition{parameters="
          + Arrays.toString(parameters)
          + ", returnType="
          + returnType
          + ", returnColumns="
          + Arrays.toString(returnColumns)
          + ", impls="
          + Arrays.toString(impls)
          + '}';
    }
  }
}
