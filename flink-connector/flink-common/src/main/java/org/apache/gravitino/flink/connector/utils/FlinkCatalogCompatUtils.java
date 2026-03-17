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

package org.apache.gravitino.flink.connector.utils;

import com.google.common.base.Preconditions;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;

/** Compatibility helpers for Flink catalog APIs that change across supported versions. */
public final class FlinkCatalogCompatUtils {

  private static final @Nullable Method CATALOG_TABLE_OF_METHOD =
      findMethod(CatalogTable.class, "of", Schema.class, String.class, List.class, Map.class);
  private static final @Nullable Method CATALOG_TABLE_NEW_BUILDER_METHOD =
      findMethod(CatalogTable.class, "newBuilder");
  private static final @Nullable Method CATALOG_TABLE_BUILDER_SCHEMA_METHOD =
      findBuilderMethod("schema", Schema.class);
  private static final @Nullable Method CATALOG_TABLE_BUILDER_COMMENT_METHOD =
      findBuilderMethod("comment", String.class);
  private static final @Nullable Method CATALOG_TABLE_BUILDER_PARTITION_KEYS_METHOD =
      findBuilderMethod("partitionKeys", List.class);
  private static final @Nullable Method CATALOG_TABLE_BUILDER_OPTIONS_METHOD =
      findBuilderMethod("options", Map.class);
  private static final @Nullable Method CATALOG_TABLE_BUILDER_BUILD_METHOD =
      findBuilderMethod("build");
  private static final @Nullable Method LEGACY_SERIALIZE_CATALOG_TABLE_METHOD =
      findMethod(CatalogPropertiesUtil.class, "serializeCatalogTable", ResolvedCatalogTable.class);
  private static final @Nullable Method MODERN_SERIALIZE_CATALOG_TABLE_METHOD =
      findModernSerializeCatalogTableMethod();
  private static final @Nullable Object DEFAULT_SQL_FACTORY_INSTANCE = findDefaultSqlFactory();

  private FlinkCatalogCompatUtils() {}

  public static CatalogTable createCatalogTable(
      Schema schema, String comment, List<String> partitionKeys, Map<String, String> options) {
    if (CATALOG_TABLE_OF_METHOD != null) {
      return (CatalogTable)
          invoke(CATALOG_TABLE_OF_METHOD, null, schema, comment, partitionKeys, options);
    }

    Preconditions.checkState(
        CATALOG_TABLE_NEW_BUILDER_METHOD != null
            && CATALOG_TABLE_BUILDER_SCHEMA_METHOD != null
            && CATALOG_TABLE_BUILDER_COMMENT_METHOD != null
            && CATALOG_TABLE_BUILDER_PARTITION_KEYS_METHOD != null
            && CATALOG_TABLE_BUILDER_OPTIONS_METHOD != null
            && CATALOG_TABLE_BUILDER_BUILD_METHOD != null,
        "No compatible CatalogTable creation path was found.");

    Object builder = invoke(CATALOG_TABLE_NEW_BUILDER_METHOD, null);
    builder = invoke(CATALOG_TABLE_BUILDER_SCHEMA_METHOD, builder, schema);
    builder = invoke(CATALOG_TABLE_BUILDER_COMMENT_METHOD, builder, comment);
    builder = invoke(CATALOG_TABLE_BUILDER_PARTITION_KEYS_METHOD, builder, partitionKeys);
    builder = invoke(CATALOG_TABLE_BUILDER_OPTIONS_METHOD, builder, options);
    return (CatalogTable) invoke(CATALOG_TABLE_BUILDER_BUILD_METHOD, builder);
  }

  @SuppressWarnings("unchecked")
  public static Map<String, String> serializeCatalogTable(ResolvedCatalogTable resolvedTable) {
    if (LEGACY_SERIALIZE_CATALOG_TABLE_METHOD != null) {
      return (Map<String, String>)
          invoke(LEGACY_SERIALIZE_CATALOG_TABLE_METHOD, null, resolvedTable);
    }

    Preconditions.checkState(
        MODERN_SERIALIZE_CATALOG_TABLE_METHOD != null && DEFAULT_SQL_FACTORY_INSTANCE != null,
        "No compatible CatalogPropertiesUtil.serializeCatalogTable path was found.");

    return (Map<String, String>)
        invoke(
            MODERN_SERIALIZE_CATALOG_TABLE_METHOD,
            null,
            resolvedTable,
            DEFAULT_SQL_FACTORY_INSTANCE);
  }

  private static @Nullable Method findBuilderMethod(String name, Class<?>... parameterTypes) {
    if (CATALOG_TABLE_NEW_BUILDER_METHOD == null) {
      return null;
    }

    return findMethod(CATALOG_TABLE_NEW_BUILDER_METHOD.getReturnType(), name, parameterTypes);
  }

  private static @Nullable Method findModernSerializeCatalogTableMethod() {
    try {
      Class<?> sqlFactoryClass = Class.forName("org.apache.flink.table.expressions.SqlFactory");
      return CatalogPropertiesUtil.class.getMethod(
          "serializeCatalogTable", ResolvedCatalogTable.class, sqlFactoryClass);
    } catch (ReflectiveOperationException e) {
      return null;
    }
  }

  private static @Nullable Object findDefaultSqlFactory() {
    if (MODERN_SERIALIZE_CATALOG_TABLE_METHOD == null) {
      return null;
    }

    try {
      Class<?> defaultSqlFactoryClass =
          Class.forName("org.apache.flink.table.expressions.DefaultSqlFactory");
      Field instanceField = defaultSqlFactoryClass.getField("INSTANCE");
      return instanceField.get(null);
    } catch (ReflectiveOperationException e) {
      return null;
    }
  }

  private static @Nullable Method findMethod(
      Class<?> targetClass, String methodName, Class<?>... parameterTypes) {
    try {
      return targetClass.getMethod(methodName, parameterTypes);
    } catch (ReflectiveOperationException e) {
      return null;
    }
  }

  private static Object invoke(Method method, @Nullable Object instance, Object... arguments) {
    try {
      return method.invoke(instance, arguments);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          String.format("Failed to invoke Flink compatibility method %s.", method), e);
    }
  }
}
