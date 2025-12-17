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
import java.util.Arrays;
import java.util.Objects;
import org.apache.gravitino.annotation.Evolving;

/** Represents a function signature, including the name and parameters. */
@Evolving
public class FunctionSignature {
  private static final FunctionParam[] EMPTY_PARAMS = new FunctionParam[0];

  private final String name;
  private final FunctionParam[] functionParams;

  private FunctionSignature(String name, FunctionParam[] functionParams) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Function name cannot be null");
    this.name = name;
    this.functionParams =
        functionParams == null
            ? EMPTY_PARAMS
            : Arrays.copyOf(functionParams, functionParams.length);
  }

  /**
   * Create a {@link FunctionSignature} instance.
   *
   * @param name The function name.
   * @param functionParams The function parameters.
   * @return A {@link FunctionSignature} instance.
   */
  public static FunctionSignature of(String name, FunctionParam... functionParams) {
    return new FunctionSignature(name, functionParams);
  }

  /**
   * @return The function name.
   */
  public String name() {
    return name;
  }

  /**
   * @return The function parameters.
   */
  public FunctionParam[] functionParams() {
    return Arrays.copyOf(functionParams, functionParams.length);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof FunctionSignature)) {
      return false;
    }
    FunctionSignature that = (FunctionSignature) obj;
    return Objects.equals(name, that.name) && Arrays.equals(functionParams, that.functionParams);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(name);
    result = 31 * result + Arrays.hashCode(functionParams);
    return result;
  }

  @Override
  public String toString() {
    return "FunctionSignature{"
        + "name='"
        + name
        + '\''
        + ", functionParams="
        + Arrays.toString(functionParams)
        + '}';
  }
}
