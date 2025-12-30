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
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/**
 * Base class of function implementations.
 *
 * <p>A function implementation must declare its language and optional external resources. Concrete
 * implementations are provided by {@link SQLImpl}, {@link JavaImpl}, and {@link PythonImpl}.
 */
@Evolving
public abstract class FunctionImpl {
  /** Supported implementation languages. */
  public enum Language {
    /** SQL implementation. */
    SQL,
    /** Java implementation. */
    JAVA,
    /** Python implementation. */
    PYTHON
  }

  /** Supported execution runtimes for function implementations. */
  public enum RuntimeType {
    /** Spark runtime. */
    SPARK,
    /** Trino runtime. */
    TRINO;

    /**
     * Parse a runtime value from string.
     *
     * @param value Runtime name.
     * @return Parsed runtime.
     * @throws IllegalArgumentException If the runtime is not supported.
     */
    public static RuntimeType fromString(String value) {
      Preconditions.checkArgument(StringUtils.isNotBlank(value), "Function runtime must be set");
      return RuntimeType.valueOf(value.trim().toUpperCase());
    }
  }

  private final Language language;
  private final RuntimeType runtime;
  private final FunctionResources resources;
  private final Map<String, String> properties;

  /**
   * Construct a {@link FunctionImpl}.
   *
   * @param language The language of the function implementation.
   * @param runtime The runtime of the function implementation.
   * @param resources The resources required by the function implementation.
   * @param properties The properties of the function implementation.
   */
  protected FunctionImpl(
      Language language,
      RuntimeType runtime,
      FunctionResources resources,
      Map<String, String> properties) {
    Preconditions.checkNotNull(language, "Function implementation language must be set");
    Preconditions.checkNotNull(runtime, "Function runtime must be set");
    this.language = language;
    this.runtime = runtime;
    this.resources = resources == null ? FunctionResources.empty() : resources;
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
  }

  /**
   * Create a SQL implementation.
   *
   * @param runtime Target runtime.
   * @param sql SQL text body.
   * @return A {@link SQLImpl} instance.
   */
  public static SQLImpl ofSql(RuntimeType runtime, String sql) {
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
      RuntimeType runtime,
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
  public static JavaImpl ofJava(RuntimeType runtime, String className) {
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
      RuntimeType runtime,
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
  public static PythonImpl ofPython(RuntimeType runtime, String handler) {
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
      RuntimeType runtime,
      String handler,
      String codeBlock,
      FunctionResources resources,
      Map<String, String> properties) {
    return new PythonImpl(runtime, handler, codeBlock, resources, properties);
  }

  /**
   * @return The implementation language.
   */
  public Language language() {
    return language;
  }

  /**
   * @return The target runtime.
   */
  public RuntimeType runtime() {
    return runtime;
  }

  /**
   * @return The external resources required by this implementation.
   */
  public FunctionResources resources() {
    return resources;
  }

  /**
   * @return The additional properties of this implementation.
   */
  public Map<String, String> properties() {
    return properties;
  }
}
