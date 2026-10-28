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
package org.apache.gravitino.auxiliary;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;

/**
 * Process-wide hook so a metalake Iceberg catalog can call the auxiliary Iceberg REST service in
 * this JVM. The holder lives on the server class loader, which both the catalog plugin and the
 * auxiliary service can see. The operations object itself is created on the auxiliary class loader,
 * next to that service's catalog wrapper, so the call does not go through HTTP.
 */
public final class IcebergAuxCatalogHooks {

  /** Builds catalog operations on the auxiliary class loader. */
  @FunctionalInterface
  public interface OperationsFactory {
    /**
     * Creates operations that use the auxiliary IRC wrapper directly.
     *
     * @return operations bound to the auxiliary class loader
     */
    CatalogOperations create();
  }

  /**
   * Initializes Iceberg operations from properties already translated on the caller's class loader.
   * Avoids casting property metadata across isolated class loaders.
   */
  public interface ResolvedInitializer {
    /**
     * Finishes initialization with Iceberg catalog properties.
     *
     * @param properties translated Iceberg catalog properties, including the catalog id
     * @param info catalog identity
     */
    void initializeResolved(Map<String, String> properties, CatalogInfo info);
  }

  private static final AtomicReference<OperationsFactory> FACTORY = new AtomicReference<>();

  private static final AtomicReference<ClassLoader> AUX_LOADER = new AtomicReference<>();

  private IcebergAuxCatalogHooks() {}

  /**
   * Installs the factory used while auxiliary IRC is running. Pass {@code null} to clear it.
   *
   * <p>The current thread's context class loader is stored and restored around later calls so
   * Hadoop and Iceberg see the auxiliary loader.
   *
   * @param factory factory that creates in-process operations, or {@code null} to clear
   */
  public static void registerOperationsFactory(@Nullable OperationsFactory factory) {
    FACTORY.set(factory);
    AUX_LOADER.set(factory == null ? null : Thread.currentThread().getContextClassLoader());
  }

  /**
   * Whether auxiliary IRC has published an in-process operations factory.
   *
   * @return true when {@link #registerOperationsFactory} installed a factory
   */
  public static boolean hasOperationsFactory() {
    return FACTORY.get() != null;
  }

  /**
   * Creates operations on the auxiliary class loader, or {@code null} when auxiliary IRC is not
   * running.
   *
   * @return operations that call the IRC wrapper in-process, or {@code null}
   */
  @Nullable
  public static CatalogOperations createOperations() {
    OperationsFactory factory = FACTORY.get();
    if (factory == null) {
      return null;
    }
    return call(factory::create);
  }

  /**
   * Runs {@code action} with the auxiliary class loader as the context class loader.
   *
   * @param action work to run
   * @param <T> result type
   * @return the action result
   */
  public static <T> T call(Callable<T> action) {
    ClassLoader aux = AUX_LOADER.get();
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    if (aux != null) {
      Thread.currentThread().setContextClassLoader(aux);
    }
    try {
      return action.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (aux != null) {
        Thread.currentThread().setContextClassLoader(previous);
      }
    }
  }
}
