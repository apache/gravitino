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
package org.apache.gravitino.iceberg.common.utils;

import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for extracting schedulers from Caffeine caches.
 *
 * <p>Provides reflection-based access to Caffeine's internal ScheduledExecutorService when caches
 * don't properly clean up their scheduler thread pools.
 *
 * <p>Reflection path (Caffeine 2.9.3): Cache → pacer → scheduler → ScheduledExecutorService
 */
public class CaffeineSchedulerExtractorUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CaffeineSchedulerExtractorUtils.class);

  private CaffeineSchedulerExtractorUtils() {
    // Utility class, prevent instantiation
  }

  /**
   * Extracts the ScheduledExecutorService from a Caffeine Cache using reflection.
   *
   * <p>Works with various cache implementations: BoundedLocalManualCache, BoundedLocalLoadingCache,
   * etc.
   *
   * @param cache The Caffeine cache instance
   * @return The ScheduledExecutorService if found, null otherwise
   */
  public static ScheduledExecutorService extractSchedulerExecutor(Object cache) {
    try {
      // Step 1: Unwrap cache if it's a wrapper (e.g., BoundedLocalManualCache)
      Object cacheImpl = unwrapCache(cache);

      // Step 2: Get Pacer from cache
      Object pacer = getPacerFromCache(cacheImpl);
      if (pacer == null) {
        return null;
      }

      // Step 3: Get Scheduler from Pacer
      Scheduler scheduler = getSchedulerFromPacer(pacer);
      if (scheduler == null) {
        return null;
      }

      // Step 4: Extract ScheduledExecutorService from Scheduler
      ScheduledExecutorService executorService = getExecutorServiceFromScheduler(scheduler);
      if (executorService != null) {
        LOG.info("Successfully extracted ScheduledExecutorService from Caffeine cache");
        return executorService;
      }

    } catch (Exception e) {
      LOG.warn("Failed to extract scheduler from Caffeine cache: {}", e.getMessage());
    }

    return null;
  }

  /**
   * Unwraps cache wrappers (e.g., BoundedLocalManualCache) to access the underlying implementation.
   *
   * @param cache The cache object (possibly a wrapper)
   * @return The unwrapped cache implementation
   */
  @VisibleForTesting
  public static Object unwrapCache(Object cache) {
    if (cache == null) {
      return null;
    }

    // Try to get inner cache field (for wrapper classes)
    try {
      Object innerCache = FieldUtils.readField(cache, "cache", true);
      if (innerCache != null) {
        return innerCache;
      }
    } catch (IllegalAccessException e) {
      // Not a wrapper, use cache directly
    }

    return cache;
  }

  /**
   * Gets the Pacer from cache implementation (traverses class hierarchy if needed).
   *
   * @param cacheImpl The cache implementation
   * @return The Pacer object, or null if not found
   */
  @VisibleForTesting
  public static Object getPacerFromCache(Object cacheImpl) {
    if (cacheImpl == null) {
      return null;
    }

    // Traverse class hierarchy to find pacer field
    Class<?> clazz = cacheImpl.getClass();
    while (clazz != null && !clazz.equals(Object.class)) {
      try {
        Object pacer = FieldUtils.readField(cacheImpl, "pacer", true);
        if (pacer != null && "Pacer".equals(pacer.getClass().getSimpleName())) {
          return pacer;
        }
      } catch (IllegalAccessException e) {
        // Continue to superclass
      }
      clazz = clazz.getSuperclass();
    }

    return null;
  }

  /**
   * Extracts Scheduler from Pacer.
   *
   * @param pacer The Pacer object
   * @return The Scheduler instance, or null if not found
   */
  @VisibleForTesting
  public static Scheduler getSchedulerFromPacer(Object pacer) {
    if (pacer == null) {
      return null;
    }

    try {
      Object scheduler = FieldUtils.readField(pacer, "scheduler", true);
      if (scheduler instanceof Scheduler) {
        return (Scheduler) scheduler;
      }
    } catch (IllegalAccessException e) {
      // Scheduler field not accessible
    }

    return null;
  }

  /**
   * Extracts ScheduledExecutorService from Scheduler (handles GuardedScheduler wrapper).
   *
   * @param scheduler The Scheduler instance
   * @return The ScheduledExecutorService, or null if not found
   */
  @VisibleForTesting
  public static ScheduledExecutorService getExecutorServiceFromScheduler(Scheduler scheduler) {
    if (scheduler == null) {
      return null;
    }

    // Handle GuardedScheduler wrapper
    Object actualScheduler = scheduler;
    if ("GuardedScheduler".equals(scheduler.getClass().getSimpleName())) {
      try {
        Object delegate = FieldUtils.readField(scheduler, "delegate", true);
        if (delegate != null) {
          actualScheduler = delegate;
        }
      } catch (IllegalAccessException e) {
        // Could not unwrap, use original
      }
    }

    // Extract from ExecutorServiceScheduler
    if ("ExecutorServiceScheduler".equals(actualScheduler.getClass().getSimpleName())) {
      try {
        Object executorService =
            FieldUtils.readField(actualScheduler, "scheduledExecutorService", true);
        if (executorService instanceof ScheduledExecutorService) {
          return (ScheduledExecutorService) executorService;
        }
      } catch (IllegalAccessException e) {
        // Could not extract
      }
    }
    return null;
  }
}
