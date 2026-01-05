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
    Preconditions.checkArgument(language != null, "Function implementation language must be set");
    this.language = language;
    Preconditions.checkArgument(runtime != null, "Function runtime must be set");
    this.runtime = runtime;
    this.resources = resources == null ? FunctionResources.empty() : resources;
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
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
