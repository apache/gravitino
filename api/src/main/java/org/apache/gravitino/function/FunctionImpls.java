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
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;

/** Helper methods to create {@link FunctionImpl} instances. */
public class FunctionImpls {

  /**
   * Copy an array of function implementations.
   *
   * @param impls The function implementations.
   * @return A copy of the input array.
   */
  public static FunctionImpl[] of(FunctionImpl... impls) {
    Preconditions.checkArgument(ArrayUtils.isNotEmpty(impls), "Impls cannot be null or empty");
    return Arrays.copyOf(impls, impls.length);
  }

  /**
   * Create a SQL implementation.
   *
   * @param runtime Target runtime.
   * @param sql SQL text body.
   * @return A {@link SQLImpl} instance.
   */
  public static SQLImpl ofSql(FunctionImpl.RuntimeType runtime, String sql) {
    return ofSql(runtime, sql, null, null);
  }

  /**
   * Create a SQL implementation.
   *
   * @param runtime Target runtime.
   * @param sql SQL text body.
   * @param resources External resources required by the implementation.
   * @param properties Additional implementation properties.
   * @return A {@link SQLImpl} instance.
   */
  public static SQLImpl ofSql(
      FunctionImpl.RuntimeType runtime,
      String sql,
      FunctionResources resources,
      Map<String, String> properties) {
    return new SQLImpl(runtime, sql, resources, properties);
  }

  /**
   * Create a Java implementation.
   *
   * @param runtime Target runtime.
   * @param className Fully qualified class name.
   * @return A {@link JavaImpl} instance.
   */
  public static JavaImpl ofJava(FunctionImpl.RuntimeType runtime, String className) {
    return ofJava(runtime, className, null, null);
  }

  /**
   * Create a Java implementation.
   *
   * @param runtime Target runtime.
   * @param className Fully qualified class name.
   * @param resources External resources required by the implementation.
   * @param properties Additional implementation properties.
   * @return A {@link JavaImpl} instance.
   */
  public static JavaImpl ofJava(
      FunctionImpl.RuntimeType runtime,
      String className,
      FunctionResources resources,
      Map<String, String> properties) {
    return new JavaImpl(runtime, className, resources, properties);
  }

  /**
   * Create a Python implementation.
   *
   * @param runtime Target runtime.
   * @param handler Python handler entrypoint.
   * @return A {@link PythonImpl} instance.
   */
  public static PythonImpl ofPython(FunctionImpl.RuntimeType runtime, String handler) {
    return ofPython(runtime, handler, null, null, null);
  }

  /**
   * Create a Python implementation.
   *
   * @param runtime Target runtime.
   * @param handler Python handler entrypoint.
   * @param codeBlock Inline code block for the handler.
   * @param resources External resources required by the implementation.
   * @param properties Additional implementation properties.
   * @return A {@link PythonImpl} instance.
   */
  public static PythonImpl ofPython(
      FunctionImpl.RuntimeType runtime,
      String handler,
      String codeBlock,
      FunctionResources resources,
      Map<String, String> properties) {
    return new PythonImpl(runtime, handler, codeBlock, resources, properties);
  }
}
