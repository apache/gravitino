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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process-wide hook so lakehouse Iceberg catalogs can reuse catalog wrappers owned by the embedded
 * (auxiliary) Iceberg REST service instead of HTTP-looping to the local REST port.
 *
 * <p>Aux {@code RESTService} registers a lookup backed by {@code IcebergCatalogWrapperManager} and
 * the local IRC base URIs. Lakehouse resolves by IRC catalog name when {@code catalog-backend=rest}
 * and {@code uri} matches a registered base URI.
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

  private static final AtomicReference<Set<String>> LOCAL_BASE_URIS =
      new AtomicReference<>(Collections.emptySet());

  private IcebergAuxCatalogBridge() {}

  /**
   * Registers the aux IRC lookup and local base URIs. Replaces any previous registration.
   *
   * @param lookup lookup backed by {@code IcebergCatalogWrapperManager}, or {@code null} to clear
   * @param baseUris HTTP(S) base URIs clients use to reach this embedded IRC (trailing slashes
   *     ignored), or {@code null}/empty when clearing
   */
  public static void register(@Nullable Lookup lookup, @Nullable Collection<String> baseUris) {
    LOOKUP.set(lookup);
    if (baseUris == null || baseUris.isEmpty()) {
      LOCAL_BASE_URIS.set(Collections.emptySet());
    } else {
      Set<String> normalized = new HashSet<>();
      for (String uri : baseUris) {
        String n = normalizeBaseUri(uri);
        if (n != null) {
          normalized.add(n);
        }
      }
      LOCAL_BASE_URIS.set(Collections.unmodifiableSet(normalized));
    }
    if (lookup == null) {
      LOG.info("Cleared Iceberg aux catalog bridge");
    } else {
      LOG.info(
          "Registered Iceberg aux catalog bridge with {} base URI(s)",
          LOCAL_BASE_URIS.get().size());
    }
  }

  /** Clears the aux IRC lookup and local URIs. */
  public static void clear() {
    register(null, null);
  }

  /**
   * Whether {@code uri} targets the embedded Iceberg REST service registered on this bridge.
   *
   * @param uri catalog {@code uri} property, may be null
   * @return true when a lookup is registered and {@code uri} matches a registered base URI
   */
  public static boolean isLocalRestUri(@Nullable String uri) {
    Set<String> bases = LOCAL_BASE_URIS.get();
    if (bases.isEmpty() || StringUtils.isBlank(uri)) {
      return false;
    }
    String normalized = normalizeBaseUri(uri);
    if (normalized == null) {
      return false;
    }
    for (String base : bases) {
      if (normalized.equals(base) || normalized.startsWith(base + "/")) {
        return true;
      }
    }
    return false;
  }

  /**
   * Resolves a shared aux IRC wrapper when the bridge is registered. Empty when unset, unknown, or
   * lookup fails with {@link NoSuchCatalogException}.
   *
   * @param catalogName IRC catalog name (warehouse selector or metalake catalog name)
   * @return shared wrapper, or empty to fall back
   */
  public static Optional<IcebergCatalogWrapper> resolve(String catalogName) {
    Lookup lookup = LOOKUP.get();
    if (lookup == null || StringUtils.isBlank(catalogName)) {
      return Optional.empty();
    }
    try {
      return Optional.ofNullable(lookup.lookup(catalogName));
    } catch (NoSuchCatalogException e) {
      LOG.debug(
          "Aux IRC has no catalog '{}'; lakehouse will fall back: {}", catalogName, e.getMessage());
      return Optional.empty();
    }
  }

  /**
   * Whether auxiliary IRC has registered a lookup. Lakehouse uses this to decide in-process IRC1
   * access.
   *
   * @return true when {@link #register} installed a lookup
   */
  public static boolean isRegistered() {
    return LOOKUP.get() != null;
  }

  @Nullable
  private static String normalizeBaseUri(@Nullable String uri) {
    if (StringUtils.isBlank(uri)) {
      return null;
    }
    String trimmed = uri.trim();
    int schemeSep = trimmed.indexOf("://");
    if (schemeSep < 0) {
      return null;
    }
    String scheme = trimmed.substring(0, schemeSep).toLowerCase(Locale.ROOT);
    String rest = trimmed.substring(schemeSep + 3);
    while (rest.endsWith("/")) {
      rest = rest.substring(0, rest.length() - 1);
    }
    return scheme + "://" + rest;
  }
}
