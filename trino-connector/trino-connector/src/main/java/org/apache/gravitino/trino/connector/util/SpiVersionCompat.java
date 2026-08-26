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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Compatibility helpers for Trino SPI methods whose signatures changed across the supported Trino
 * versions.
 *
 * <p>These are outbound calls (the connector reading Trino SPI objects) on public SPI types used by
 * the shared connector source, which every version-segment module compiles. Because the signatures
 * differ between versions they cannot be referenced at compile time against a single shape, so they
 * are resolved reflectively by name and cached. The fixed metadata-path methods are cached in
 * static fields; the generic {@link #invoke} helper caches its resolved {@link Method} per {@code
 * (class, name, parameter types)} in a concurrent map, so its usages on the split and
 * page-source/page-sink path do a hash lookup rather than a reflective method scan per call.
 */
public final class SpiVersionCompat {

  private static final Method COLUMN_GET_COMMENT = resolve(ColumnMetadata.class, "getComment");
  private static final Method SCHEMA_FUNCTION_SCHEMA_NAME =
      resolveFirst(SchemaFunctionName.class, "schemaName", "getSchemaName");
  private static final Method SCHEMA_FUNCTION_FUNCTION_NAME =
      resolveFirst(SchemaFunctionName.class, "functionName", "getFunctionName");

  private static final Map<MethodKey, Method> METHOD_CACHE = new ConcurrentHashMap<>();

  private SpiVersionCompat() {}

  /** Cache key identifying a reflectively resolved method by its declaring class and signature. */
  private record MethodKey(
      Class<?> targetClass, String methodName, List<Class<?>> parameterTypes) {}

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

  /**
   * Invokes a Trino SPI method reflectively on the given target. This is used for outbound calls to
   * SPI overloads whose signatures differ across supported Trino versions (for example split and
   * page-source/page-sink methods reworked in Trino 482), so the shared connector source compiles
   * against every version and dispatches to the overload actually present at runtime. The resolved
   * {@link Method} is cached per {@code (target class, name, parameter types)}, because these calls
   * are on the split and page-source/page-sink path (per split/task), so repeated invocations avoid
   * a fresh reflective method scan.
   *
   * @param target the object to invoke the method on
   * @param methodName the SPI method name
   * @param parameterTypes the method parameter types
   * @param args the arguments to pass
   * @return the method result
   */
  public static Object invoke(
      Object target, String methodName, Class<?>[] parameterTypes, Object... args) {
    Method method =
        METHOD_CACHE.computeIfAbsent(
            new MethodKey(target.getClass(), methodName, List.of(parameterTypes)),
            SpiVersionCompat::resolve);
    try {
      return method.invoke(target, args);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Failed invoking Trino SPI method " + target.getClass().getName() + "#" + methodName, e);
    }
  }

  private static Method resolve(MethodKey key) {
    try {
      return key.targetClass()
          .getMethod(key.methodName(), key.parameterTypes().toArray(new Class<?>[0]));
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(
          "Trino SPI method "
              + key.targetClass().getName()
              + "#"
              + key.methodName()
              + " was not found",
          e);
    }
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
