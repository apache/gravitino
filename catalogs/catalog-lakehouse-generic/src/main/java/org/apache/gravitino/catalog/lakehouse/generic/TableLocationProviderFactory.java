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
package org.apache.gravitino.catalog.lakehouse.generic;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory that discovers {@link TableLocationProvider}s through {@link ServiceLoader}.
 *
 * <p>Discovery is done once per class loader and remembered, because it is the expensive half:
 * selecting by name requires every registered provider to be instantiated so that {@link
 * TableLocationProvider#name()} can be called on it, and {@code name()} is an instance method, so
 * there is no way to learn the names without doing that at least once. What the cache removes is
 * repeating it for every catalog; it cannot remove the first pass. Creating a catalog after the
 * first then instantiates only the provider it selected.
 *
 * <p>The remembered index holds classes, and a class keeps its class loader alive, so the entries
 * are weak on both sides: the map is keyed weakly by class loader and holds each class through a
 * {@link WeakReference}. A strong value would pin the loader of a dropped catalog through the very
 * map meant to speed the next one up, which is the leak {@code [#12986]} removed elsewhere. A
 * collected entry simply causes the next lookup to scan again.
 */
public class TableLocationProviderFactory {

  private static final Logger LOG = LoggerFactory.getLogger(TableLocationProviderFactory.class);

  /**
   * Provider classes by lower-cased name, per class loader. Weak on both sides; see the class
   * javadoc. Guarded by synchronization on the map itself rather than by a concurrent map, because
   * {@link WeakHashMap} is not thread-safe and the map is touched once per catalog creation.
   */
  private static final Map<ClassLoader, Index> INDEXES =
      Collections.synchronizedMap(new WeakHashMap<>());

  private TableLocationProviderFactory() {}

  /**
   * Creates and initializes the {@link TableLocationProvider} registered under the given name.
   *
   * <p>A new instance is returned on every call, so the caller owns its lifecycle and is
   * responsible for closing it. Only the selected provider is instantiated once the class loader
   * has been scanned; the instances made during that first scan are discarded without {@link
   * TableLocationProvider#close()} being called on them, which is safe only because the interface
   * requires a constructor that acquires nothing. Everything worth closing is acquired in {@link
   * TableLocationProvider#initialize(Map)}, which only the selected provider ever reaches.
   *
   * @param name the provider name to look up, matched case-insensitively against {@link
   *     TableLocationProvider#name()}
   * @param catalogProperties the properties of the catalog the provider belongs to
   * @return the initialized provider
   * @throws IllegalArgumentException if no provider, or more than one provider, is registered under
   *     the given name, or if the selected provider cannot be instantiated
   * @throws ServiceConfigurationError if a {@code META-INF/services} file for this interface is
   *     itself malformed, which fails the scan before any candidate is reached
   */
  public static TableLocationProvider create(String name, Map<String, String> catalogProperties) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "Table location provider name must not be blank");

    ClassLoader cl =
        Optional.ofNullable(Thread.currentThread().getContextClassLoader())
            .orElse(TableLocationProvider.class.getClassLoader());
    String key = name.toLowerCase(Locale.ROOT);

    Index index = index(cl);
    Class<? extends TableLocationProvider> type = index.get(key);
    if (type == null) {
      // Either never seen, or the entry was collected along with its class loader. Scanning again
      // is the correct answer to both, and tells a caller asking for a provider added since the
      // last scan about it.
      index = rescan(cl);
      type = index.get(key);
    }

    Preconditions.checkArgument(
        !index.isDuplicated(key),
        "Multiple TableLocationProviders are registered under the name '%s'. Provider names must "
            + "be unique across the classpath.",
        name);
    Preconditions.checkArgument(type != null, "No TableLocationProvider found for name '%s'", name);

    TableLocationProvider provider = instantiate(type);
    try {
      // Passed on as given rather than copied again. The caller hands over a map it already
      // made unmodifiable, and copying it here would only re-introduce the rejection of a null
      // property value that the caller deliberately tolerates.
      provider.initialize(catalogProperties == null ? Map.of() : catalogProperties);
    } catch (RuntimeException | Error e) {
      // initialize is where a provider opens its clients and connections, so one that fails
      // halfway has resources to release. Nobody else can do it: the instance never reaches the
      // caller that would have owned its lifecycle. Error is caught alongside RuntimeException
      // because a plugin loaded through its own classloader fails with NoClassDefFoundError as
      // readily as with an exception, and it is rethrown either way.
      closeQuietly(provider, name);
      throw e;
    }

    LOG.info("Loaded TableLocationProvider '{}': {}", name, type.getName());
    return provider;
  }

  /**
   * Forgets everything discovered so far, so that the next lookup scans again. For tests, which
   * register providers through class loaders they build themselves.
   */
  @VisibleForTesting
  static void invalidateCache() {
    INDEXES.clear();
  }

  private static Index index(ClassLoader cl) {
    Index index = INDEXES.get(cl);
    return index == null ? rescan(cl) : index;
  }

  /**
   * Instantiates every registered provider once to learn its name, and remembers the resulting
   * name-to-class mapping.
   *
   * <p>A candidate that cannot be instantiated, or that cannot report its name, is logged and
   * skipped rather than allowed to fail the scan: one broken third-party jar on the classpath must
   * not stop every catalog from starting, including the ones on the built-in provider. A services
   * file that is itself malformed still fails the scan; that error is raised while the loader is
   * being iterated, before any candidate is reached.
   *
   * <p>A name claimed by more than one provider is recorded rather than thrown here, and fails only
   * the catalogs that actually ask for that name. Throwing during the scan would let two unrelated
   * third-party jars break every catalog, which is the same failure the paragraph above avoids.
   *
   * @param cl the class loader to scan
   * @return the index for that class loader
   */
  private static Index rescan(ClassLoader cl) {
    Map<String, WeakReference<Class<? extends TableLocationProvider>>> byName = new HashMap<>();
    Set<String> duplicated = new HashSet<>();

    List<ServiceLoader.Provider<TableLocationProvider>> candidates =
        ServiceLoader.load(TableLocationProvider.class, cl).stream().collect(Collectors.toList());

    for (ServiceLoader.Provider<TableLocationProvider> candidate : candidates) {
      Class<? extends TableLocationProvider> type = candidate.type();
      String name;
      try {
        name = candidate.get().name();
      } catch (RuntimeException | Error e) {
        // Error, not just ServiceConfigurationError: the loader wraps a failing constructor for
        // us, but name() is called here rather than by the loader, so whatever it throws arrives
        // unwrapped. A plugin loaded through its own class loader reaches a missing class with a
        // NoClassDefFoundError, which is an Error, and letting it through would fail the whole
        // scan.
        LOG.warn(
            "Skipping TableLocationProvider {}, which could not be instantiated or could not "
                + "report its name. It cannot be selected by any catalog until this is fixed.",
            type.getName(),
            e);
        continue;
      }

      if (StringUtils.isBlank(name)) {
        LOG.warn(
            "Skipping TableLocationProvider {}, which reported a null or blank name.",
            type.getName());
        continue;
      }

      String key = name.toLowerCase(Locale.ROOT);
      WeakReference<Class<? extends TableLocationProvider>> previous =
          byName.put(key, new WeakReference<>(type));
      if (previous != null && previous.get() != type) {
        duplicated.add(key);
        LOG.warn(
            "TableLocationProvider name '{}' is claimed by more than one implementation, including "
                + "{} and {}. Catalogs selecting that name will fail to initialize.",
            name,
            previous.get() == null ? "an unloaded class" : previous.get().getName(),
            type.getName());
      }
    }

    Index index = new Index(byName, duplicated);
    INDEXES.put(cl, index);
    LOG.info("Discovered TableLocationProviders: {}", byName.keySet());
    return index;
  }

  private static TableLocationProvider instantiate(Class<? extends TableLocationProvider> type) {
    try {
      return type.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException | RuntimeException | Error e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to instantiate TableLocationProvider %s. It must have a public no-argument "
                  + "constructor that does not throw.",
              type.getName()),
          e);
    }
  }

  private static void closeQuietly(TableLocationProvider provider, String name) {
    try {
      provider.close();
    } catch (Exception | Error e) {
      // Error too: this runs while the initialize failure is being unwound, and a close that threw
      // one would replace the failure the caller actually needs to see.
      LOG.warn(
          "Failed to close TableLocationProvider '{}' after it failed to initialize. Any resource "
              + "it acquired before failing may be leaked.",
          name,
          e);
    }
  }

  /** The providers discovered under one class loader, held weakly. */
  private static final class Index {

    private final Map<String, WeakReference<Class<? extends TableLocationProvider>>> byName;

    private final Set<String> duplicated;

    private Index(
        Map<String, WeakReference<Class<? extends TableLocationProvider>>> byName,
        Set<String> duplicated) {
      this.byName = byName;
      this.duplicated = duplicated;
    }

    private Class<? extends TableLocationProvider> get(String key) {
      WeakReference<Class<? extends TableLocationProvider>> ref = byName.get(key);
      return ref == null ? null : ref.get();
    }

    private boolean isDuplicated(String key) {
      return duplicated.contains(key);
    }
  }
}
