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
package org.apache.gravitino.trino.connector.util;

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.function.SchemaFunctionName;
import java.lang.reflect.Method;
import java.util.Optional;

/**
 * Compatibility helpers for Trino SPI methods whose signatures changed across the supported Trino
 * versions.
 *
 * <p>These are outbound calls (the connector reading Trino SPI objects) on public SPI types used by
 * the shared connector source, which every version-segment module compiles. Because the signatures
 * differ between versions they cannot be referenced at compile time against a single shape, so they
 * are resolved reflectively by name once and cached. All usages are on the low-frequency metadata
 * path, so the reflective indirection is not performance sensitive.
 */
public final class SpiVersionCompat {

  private static final Method COLUMN_GET_COMMENT = resolve(ColumnMetadata.class, "getComment");
  private static final Method SCHEMA_FUNCTION_SCHEMA_NAME =
      resolveFirst(SchemaFunctionName.class, "schemaName", "getSchemaName");
  private static final Method SCHEMA_FUNCTION_FUNCTION_NAME =
      resolveFirst(SchemaFunctionName.class, "functionName", "getFunctionName");

  private SpiVersionCompat() {}

  /**
   * Returns the comment of a Trino {@link ColumnMetadata}. Trino versions up to 479 return a {@code
   * String}; Trino 480 and later return an {@code Optional<String>}.
   *
   * @param column the Trino column metadata
   * @return the column comment, or {@code null} if there is none
   */
  public static String columnComment(ColumnMetadata column) {
    Object result = invoke(COLUMN_GET_COMMENT, column);
    if (result instanceof Optional<?> optional) {
      return (String) optional.orElse(null);
    }
    return (String) result;
  }

  /**
   * Returns the schema name of a {@link SchemaFunctionName}. Trino versions up to 479 expose {@code
   * getSchemaName()}; Trino 480 and later expose the record accessor {@code schemaName()}.
   *
   * @param name the schema function name
   * @return the schema name
   */
  public static String schemaName(SchemaFunctionName name) {
    return (String) invoke(SCHEMA_FUNCTION_SCHEMA_NAME, name);
  }

  /**
   * Returns the function name of a {@link SchemaFunctionName}. Trino versions up to 479 expose
   * {@code getFunctionName()}; Trino 480 and later expose the record accessor {@code
   * functionName()}.
   *
   * @param name the schema function name
   * @return the function name
   */
  public static String functionName(SchemaFunctionName name) {
    return (String) invoke(SCHEMA_FUNCTION_FUNCTION_NAME, name);
  }

  private static Method resolve(Class<?> type, String method) {
    try {
      return type.getMethod(method);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(
          "Trino SPI method " + type.getName() + "#" + method + " was not found", e);
    }
  }

  private static Method resolveFirst(Class<?> type, String... candidates) {
    for (String candidate : candidates) {
      try {
        return type.getMethod(candidate);
      } catch (NoSuchMethodException ignored) {
        // try the next candidate name
      }
    }
    throw new IllegalStateException(
        "None of ["
            + String.join(", ", candidates)
            + "] were found on Trino SPI type "
            + type.getName());
  }

  private static Object invoke(Method method, Object target) {
    try {
      return method.invoke(target);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed invoking Trino SPI method " + method, e);
    }
  }
}
