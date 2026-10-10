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
package org.apache.gravitino.job.k8s;

import com.google.common.collect.ImmutableMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Caches the resources listed in a namespace for a short time, so that querying the status of many
 * jobs in the same namespace costs one LIST call instead of one GET call per job.
 *
 * <p>A resource missing from the cache isn't necessarily gone: it may have been created after the
 * namespace was listed, or the namespace can't be listed at all. The caller must fall back to a GET
 * call on a cache miss.
 *
 * @param <T> the resource type
 */
final class NamespacedResourceCache<T extends HasMetadata> {

  private static final class Snapshot<T> {

    private final long listedAtMs;

    private final Map<String, T> resources;

    private Snapshot(long listedAtMs, Map<String, T> resources) {
      this.listedAtMs = listedAtMs;
      this.resources = resources;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(NamespacedResourceCache.class);

  private final long ttlMs;

  private final Function<String, List<T>> lister;

  private final LongSupplier clock;

  private final Map<String, Snapshot<T>> snapshots = new ConcurrentHashMap<>();

  // Listing a namespace is a network call, so it is guarded by a lock of its own rather than by
  // the snapshot map, which must never block.
  private final Map<String, Object> listLocks = new ConcurrentHashMap<>();

  // Bumped by every invalidation, so that a listing started before it isn't cached.
  private final AtomicLong invalidations = new AtomicLong();

  /**
   * Creates a cache.
   *
   * @param ttlMs how long a namespace listing is cached, caching is disabled if not positive
   * @param lister lists the resources of a namespace
   * @param clock the current time in milliseconds
   */
  NamespacedResourceCache(long ttlMs, Function<String, List<T>> lister, LongSupplier clock) {
    this.ttlMs = ttlMs;
    this.lister = lister;
    this.clock = clock;
  }

  /**
   * Finds a resource in the cached listing of its namespace, and lists the namespace again if the
   * cached listing has expired. A namespace that can't be listed, for example because listing it is
   * forbidden, isn't listed again until the cached failure expires.
   *
   * @param namespace the namespace of the resource
   * @param name the name of the resource
   * @return the resource, or empty if it isn't in the listing or the namespace can't be listed
   */
  Optional<T> find(String namespace, String name) {
    if (ttlMs <= 0) {
      return Optional.empty();
    }

    Snapshot<T> snapshot = snapshots.get(namespace);
    if (isExpired(snapshot)) {
      // Only one thread lists a namespace at a time, the others wait for its listing.
      synchronized (listLocks.computeIfAbsent(namespace, ns -> new Object())) {
        snapshot = snapshots.get(namespace);
        if (isExpired(snapshot)) {
          long invalidationsBefore = invalidations.get();
          snapshot = snapshotOf(namespace);
          if (invalidations.get() == invalidationsBefore) {
            snapshots.put(namespace, snapshot);
          }
        }
      }
    }
    return Optional.ofNullable(snapshot.resources.get(name));
  }

  /**
   * Drops the cached listing of a namespace, after a resource in it is changed.
   *
   * @param namespace the namespace
   */
  void invalidate(String namespace) {
    invalidations.incrementAndGet();
    snapshots.remove(namespace);
  }

  private boolean isExpired(Snapshot<T> snapshot) {
    return snapshot == null || clock.getAsLong() - snapshot.listedAtMs >= ttlMs;
  }

  private Snapshot<T> snapshotOf(String namespace) {
    long listedAtMs = clock.getAsLong();
    try {
      ImmutableMap.Builder<String, T> resources = ImmutableMap.builder();
      lister.apply(namespace).forEach(r -> resources.put(r.getMetadata().getName(), r));
      return new Snapshot<>(listedAtMs, resources.buildKeepingLast());
    } catch (RuntimeException e) {
      // Cache the failure as an empty listing, so that the callers get the resources one by one
      // instead of each of them failing to list the namespace again.
      LOG.warn(
          "Failed to list the resources in namespace {}, getting them one by one for the next {}"
              + " ms: {}",
          namespace,
          ttlMs,
          e.toString());
      return new Snapshot<>(listedAtMs, ImmutableMap.of());
    }
  }
}
