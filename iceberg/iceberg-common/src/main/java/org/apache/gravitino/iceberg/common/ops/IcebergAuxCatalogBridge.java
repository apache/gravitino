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
package org.apache.gravitino.iceberg.common.ops;

import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process-wide hook so lakehouse Iceberg catalogs can reuse catalog wrappers owned by the embedded
 * (auxiliary) Iceberg REST service instead of constructing a second backend catalog in the same
 * JVM.
 *
 * <p>Aux {@code RESTService} registers a lookup backed by {@code IcebergCatalogWrapperManager}.
 * Lakehouse {@code IcebergCatalogOperations} resolves by metalake catalog name when present;
 * otherwise it keeps self-loading via {@link IcebergCatalogWrapper}.
 */
public final class IcebergAuxCatalogBridge {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergAuxCatalogBridge.class);

  /** Resolves an aux IRC catalog wrapper by catalog / warehouse name. */
  @FunctionalInterface
  public interface Lookup {
    /**
     * Returns the shared wrapper for {@code catalogName}.
     *
     * @param catalogName metalake / IRC catalog name
     * @return wrapper owned by aux IRC
     * @throws NoSuchCatalogException if aux IRC has no config for the name
     */
    IcebergCatalogWrapper lookup(String catalogName);
  }

  private static final AtomicReference<Lookup> LOOKUP = new AtomicReference<>();

  private IcebergAuxCatalogBridge() {}

  /**
   * Registers the aux IRC lookup. Replaces any previous registration.
   *
   * @param lookup lookup backed by {@code IcebergCatalogWrapperManager}, or {@code null} to clear
   */
  public static void register(@Nullable Lookup lookup) {
    LOOKUP.set(lookup);
    if (lookup == null) {
      LOG.info("Cleared Iceberg aux catalog bridge");
    } else {
      LOG.info("Registered Iceberg aux catalog bridge");
    }
  }

  /** Clears the aux IRC lookup. */
  public static void clear() {
    register(null);
  }

  /**
   * Resolves a shared aux IRC wrapper when the bridge is registered and the catalog name is known
   * to IRC. Empty when unset, unknown, or lookup fails with {@link NoSuchCatalogException}.
   *
   * @param catalogName metalake catalog name
   * @return shared wrapper, or empty to fall back to self-load
   */
  public static Optional<IcebergCatalogWrapper> resolve(String catalogName) {
    Lookup lookup = LOOKUP.get();
    if (lookup == null || catalogName == null || catalogName.isEmpty()) {
      return Optional.empty();
    }
    try {
      IcebergCatalogWrapper wrapper = lookup.lookup(catalogName);
      return Optional.ofNullable(wrapper);
    } catch (NoSuchCatalogException e) {
      LOG.debug(
          "Aux IRC has no catalog '{}'; lakehouse will self-load: {}", catalogName, e.getMessage());
      return Optional.empty();
    }
  }

  @VisibleForTesting
  static boolean isRegistered() {
    return LOOKUP.get() != null;
  }
}
