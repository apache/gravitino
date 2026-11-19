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
package org.apache.gravitino.iceberg.service.restriction;

import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.slf4j.LoggerFactory;

/** Optional extension that adds resolved read restrictions to an Iceberg load-table response. */
public interface IcebergReadRestrictionLoader {

  /**
   * Returns whether this loader may vary a load-table response independently of table metadata.
   *
   * <p>When enabled, the REST endpoint bypasses its metadata-location-only conditional GET fast
   * path and resolves the complete response before evaluating the ETag.
   *
   * @return {@code true} when read restrictions may be loaded
   */
  default boolean enabled() {
    return true;
  }

  /**
   * Resolves and adds read restrictions for one authenticated load-table request.
   *
   * @param context authenticated Iceberg request context
   * @param tableIdentifier Iceberg table identifier
   * @param response response produced by the catalog and any snapshot filtering
   * @return the decorated response and an ETag representation seed
   */
  Result load(
      IcebergRequestContext context, TableIdentifier tableIdentifier, LoadTableResponse response);

  /**
   * Restores response fields supported by an optional Iceberg distribution after snapshot
   * filtering.
   *
   * @param original the response before snapshot filtering
   * @param filtered the response with historical snapshots removed
   * @return the filtered response with supported fields preserved
   */
  default LoadTableResponse preserveSnapshotFilteredFields(
      LoadTableResponse original, LoadTableResponse filtered) {
    return filtered;
  }

  /**
   * Discovers the single optional loader on the runtime classpath.
   *
   * @return discovered loader, or a no-op loader when the runtime has no implementation
   */
  static IcebergReadRestrictionLoader load() {
    Iterator<IcebergReadRestrictionLoader> providers =
        ServiceLoader.load(IcebergReadRestrictionLoader.class).iterator();
    if (!providers.hasNext()) {
      return NoOpHolder.INSTANCE;
    }
    IcebergReadRestrictionLoader loader = providers.next();
    if (providers.hasNext()) {
      throw new IllegalStateException(
          "Multiple Iceberg read-restriction loaders found on the runtime classpath");
    }
    if (!loader.enabled()) {
      LoggerFactory.getLogger(IcebergReadRestrictionLoader.class)
          .warn(
              "Iceberg read-restriction loader {} is installed but disabled; read restrictions will"
                  + " not be applied. Check the loader's deployment prerequisites, including policy"
                  + " dispatcher availability.",
              loader.getClass().getName());
    }
    return loader;
  }

  /** Result of resolving read restrictions for one load-table response. */
  final class Result {
    private final LoadTableResponse response;
    private final Optional<String> etagSeed;

    private Result(LoadTableResponse response, Optional<String> etagSeed) {
      this.response = Objects.requireNonNull(response, "response cannot be null");
      this.etagSeed = Objects.requireNonNull(etagSeed, "etagSeed cannot be null");
    }

    /**
     * Creates a result.
     *
     * @param response resolved load-table response
     * @param etagSeed optional canonical seed for response state not represented by table metadata
     * @return a new result
     */
    public static Result of(LoadTableResponse response, Optional<String> etagSeed) {
      return new Result(response, etagSeed);
    }

    /**
     * @return resolved load-table response
     */
    public LoadTableResponse response() {
      return response;
    }

    /**
     * @return canonical ETag representation seed, or empty when unrestricted
     */
    public Optional<String> etagSeed() {
      return etagSeed;
    }
  }

  /** Lazily initialized no-op loader. */
  final class NoOpHolder {
    private static final IcebergReadRestrictionLoader INSTANCE =
        new IcebergReadRestrictionLoader() {
          @Override
          public boolean enabled() {
            return false;
          }

          @Override
          public Result load(
              IcebergRequestContext context,
              TableIdentifier tableIdentifier,
              LoadTableResponse response) {
            return Result.of(response, Optional.empty());
          }
        };

    private NoOpHolder() {}
  }
}
