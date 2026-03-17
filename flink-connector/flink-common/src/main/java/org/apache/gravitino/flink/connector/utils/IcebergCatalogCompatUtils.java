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
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.factories.CatalogFactory;

/** Compatibility helpers for Iceberg Flink catalog APIs across supported Flink lanes. */
public final class IcebergCatalogCompatUtils {

  private static final String FLINK_CATALOG_FACTORY_CLASS =
      "org.apache.iceberg.flink.FlinkCatalogFactory";

  private static final @Nullable Class<?> ICEBERG_FACTORY_CLASS =
      loadClass(FLINK_CATALOG_FACTORY_CLASS);

  private static final @Nullable Method LEGACY_CREATE_CATALOG_METHOD =
      findMethod(ICEBERG_FACTORY_CLASS, "createCatalog", String.class, Map.class);

  private static final @Nullable Method CONTEXT_CREATE_CATALOG_METHOD =
      findMethod(ICEBERG_FACTORY_CLASS, "createCatalog", CatalogFactory.Context.class);

  private IcebergCatalogCompatUtils() {}

  public static AbstractCatalog createIcebergCatalog(
      String catalogName, Map<String, String> catalogProperties, CatalogFactory.Context context) {
    Preconditions.checkState(
        ICEBERG_FACTORY_CLASS != null, "Iceberg Flink catalog factory class is not available.");

    Object factory = instantiateFactory();
    Object catalog;
    if (LEGACY_CREATE_CATALOG_METHOD != null) {
      catalog = invoke(LEGACY_CREATE_CATALOG_METHOD, factory, catalogName, catalogProperties);
    } else {
      Preconditions.checkState(
          CONTEXT_CREATE_CATALOG_METHOD != null,
          "No compatible Iceberg Flink catalog factory method was found.");
      catalog =
          invoke(
              CONTEXT_CREATE_CATALOG_METHOD,
              factory,
              new DelegatingCatalogContext(context, catalogName, catalogProperties));
    }

    Preconditions.checkState(
        catalog instanceof AbstractCatalog,
        "Unexpected Iceberg catalog type: %s",
        catalog == null ? "null" : catalog.getClass().getName());
    return (AbstractCatalog) catalog;
  }

  private static Object instantiateFactory() {
    try {
      return ICEBERG_FACTORY_CLASS.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to instantiate Iceberg Flink catalog factory.", e);
    }
  }

  private static @Nullable Class<?> loadClass(String className) {
    try {
      return Class.forName(className);
    } catch (ReflectiveOperationException e) {
      return null;
    }
  }

  private static @Nullable Method findMethod(
      @Nullable Class<?> targetClass, String methodName, Class<?>... parameterTypes) {
    if (targetClass == null) {
      return null;
    }

    try {
      return targetClass.getMethod(methodName, parameterTypes);
    } catch (ReflectiveOperationException e) {
      return null;
    }
  }

  private static Object invoke(Method method, Object instance, Object... arguments) {
    try {
      return method.invoke(instance, arguments);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          String.format("Failed to invoke Iceberg compatibility method %s.", method), e);
    }
  }

  private static final class DelegatingCatalogContext implements CatalogFactory.Context {
    private final CatalogFactory.Context delegate;
    private final String name;
    private final Map<String, String> options;

    private DelegatingCatalogContext(
        CatalogFactory.Context delegate, String name, Map<String, String> options) {
      this.delegate = delegate;
      this.name = name;
      this.options = Collections.unmodifiableMap(new HashMap<>(options));
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public Map<String, String> getOptions() {
      return options;
    }

    @Override
    public ReadableConfig getConfiguration() {
      return delegate.getConfiguration();
    }

    @Override
    public ClassLoader getClassLoader() {
      return delegate.getClassLoader();
    }
  }
}
