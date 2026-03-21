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

import javax.annotation.Nullable;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.Factory;

/** Compatibility helpers for JDBC catalog classes that moved packages in Flink 2.x. */
public final class JdbcCatalogCompatUtils {

  private static final String[] JDBC_CATALOG_FACTORY_CLASS_NAMES = {
    "org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactory",
    "org.apache.flink.connector.jdbc.core.database.catalog.factory.JdbcCatalogFactory"
  };
  private static final String[] JDBC_CORE_CATALOG_FACTORY_CLASS_NAMES = {
    "org.apache.flink.connector.jdbc.core.database.catalog.factory.JdbcCatalogFactory"
  };
  private static final String[] JDBC_DYNAMIC_TABLE_FACTORY_CLASS_NAMES = {
    "org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory",
    "org.apache.flink.connector.jdbc.core.table.JdbcDynamicTableFactory"
  };

  private JdbcCatalogCompatUtils() {}

  public static AbstractCatalog createJdbcCatalog(CatalogFactory.Context context) {
    Object jdbcCatalogFactory = instantiate(loadCompatibleClass(JDBC_CATALOG_FACTORY_CLASS_NAMES));
    return createJdbcCatalog(context, jdbcCatalogFactory);
  }

  public static AbstractCatalog createCoreJdbcCatalog(CatalogFactory.Context context) {
    Object jdbcCatalogFactory =
        instantiate(loadCompatibleClass(JDBC_CORE_CATALOG_FACTORY_CLASS_NAMES));
    return createJdbcCatalog(context, jdbcCatalogFactory);
  }

  private static AbstractCatalog createJdbcCatalog(
      CatalogFactory.Context context, Object jdbcCatalogFactory) {
    Object jdbcCatalog =
        invoke(
            jdbcCatalogFactory,
            "createCatalog",
            new Class<?>[] {CatalogFactory.Context.class},
            context);
    return (AbstractCatalog) jdbcCatalog;
  }

  public static Factory createJdbcDynamicTableFactory() {
    return (Factory) instantiate(loadCompatibleClass(JDBC_DYNAMIC_TABLE_FACTORY_CLASS_NAMES));
  }

  private static Class<?> loadCompatibleClass(String[] classNames) {
    for (String className : classNames) {
      try {
        return Class.forName(className);
      } catch (ClassNotFoundException e) {
        // Try the next compatible class name.
      }
    }

    throw new IllegalStateException(
        String.format(
            "No compatible JDBC catalog class was found in %s.", String.join(", ", classNames)));
  }

  private static Object instantiate(Class<?> targetClass) {
    try {
      return targetClass.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          String.format(
              "Failed to instantiate JDBC compatibility class %s.", targetClass.getName()),
          e);
    }
  }

  private static Object invoke(
      Object target, String methodName, Class<?>[] parameterTypes, @Nullable Object... arguments) {
    try {
      return target.getClass().getMethod(methodName, parameterTypes).invoke(target, arguments);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          String.format(
              "Failed to invoke JDBC compatibility method %s on %s.",
              methodName, target.getClass().getName()),
          e);
    }
  }
}
