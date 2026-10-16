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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import javax.annotation.Nullable;
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

  private final Predicate<RuntimeException> isFatal;

  private final LongSupplier clock;

  // The listing of each namespace, being made or made. Only one thread lists a namespace at a
  // time, the others wait for its listing. A listing is a network call, so it is made outside of
  // the map, which only holds the namespaces listed within the TTL or being listed.
  private final Map<String, CompletableFuture<Snapshot<T>>> snapshots = new ConcurrentHashMap<>();

  /**
   * Creates a cache.
   *
   * @param ttlMs how long a namespace listing is cached, caching is disabled if not positive
   * @param lister lists the resources of a namespace
   * @param isFatal tells the failures of the lister that getting a resource by itself would run
   *     into as well, for example an API server that can't be reached. They are thrown to the
   *     caller of {@link #find(String, String)} instead of being cached as an empty listing.
   * @param clock the current time in milliseconds
   */
  NamespacedResourceCache(
      long ttlMs,
      Function<String, List<T>> lister,
      Predicate<RuntimeException> isFatal,
      LongSupplier clock) {
    this.ttlMs = ttlMs;
    this.lister = lister;
    this.isFatal = isFatal;
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
   * @throws RuntimeException the failure to list the namespace, if it is a fatal one
   */
  Optional<T> find(String namespace, String name) {
    if (ttlMs <= 0) {
      return Optional.empty();
    }

    CompletableFuture<Snapshot<T>> snapshot = snapshots.get(namespace);
    if (isExpired(snapshot)) {
      // Drop the expired listings, also of the namespaces whose jobs are all done.
      snapshots.values().removeIf(this::isExpired);
      CompletableFuture<Snapshot<T>> listing = new CompletableFuture<>();
      snapshot =
          snapshots.compute(namespace, (ns, current) -> isExpired(current) ? listing : current);
      if (snapshot == listing) {
        try {
          listing.complete(snapshotOf(namespace));
        } catch (Throwable t) {
          // Whatever the failure, even an error, the listing must be completed and isn't cached,
          // otherwise the next queries of the namespace would wait for it forever. The waiting
          // callers get the failure too.
          snapshots.remove(namespace, listing);
          listing.completeExceptionally(t);
          throw t;
        }
      }
    }

    try {
      return Optional.ofNullable(snapshot.join().resources.get(name));
    } catch (CompletionException e) {
      if (e.getCause() instanceof Error) {
        throw (Error) e.getCause();
      }
      throw e.getCause() instanceof RuntimeException ? (RuntimeException) e.getCause() : e;
    }
  }

  /**
   * Drops the cached listing of a namespace, after a resource in it is changed. A listing of it
   * being made isn't cached either, as it may predate the change.
   *
   * @param namespace the namespace
   */
  void invalidate(String namespace) {
    snapshots.remove(namespace);
  }

  /** Whether a listing must be made again: there is none, it failed, or it is too old. */
  private boolean isExpired(@Nullable CompletableFuture<Snapshot<T>> snapshot) {
    if (snapshot == null || snapshot.isCompletedExceptionally()) {
      return true;
    }
    // A listing being made is waited for.
    Snapshot<T> made = snapshot.getNow(null);
    return made != null && clock.getAsLong() - made.listedAtMs >= ttlMs;
  }

  private Snapshot<T> snapshotOf(String namespace) {
    long listedAtMs = clock.getAsLong();
    try {
      ImmutableMap.Builder<String, T> resources = ImmutableMap.builder();
      lister.apply(namespace).forEach(r -> resources.put(r.getMetadata().getName(), r));
      return new Snapshot<>(listedAtMs, resources.buildKeepingLast());
    } catch (RuntimeException e) {
      if (isFatal.test(e)) {
        throw e;
      }
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
