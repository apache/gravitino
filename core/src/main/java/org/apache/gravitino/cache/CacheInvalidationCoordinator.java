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

package org.apache.gravitino.cache;

import java.io.Closeable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.storage.relational.service.CacheInvalidationVersionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Polls database version and invalidates the local cache when changes are detected. */
public class CacheInvalidationCoordinator implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(CacheInvalidationCoordinator.class);

  private final EntityCache cache;
  private final CacheInvalidationVersionService versionService;
  private final long pollIntervalMs;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final boolean enabled;

  private volatile long localVersion;
  private ScheduledExecutorService scheduler;

  public CacheInvalidationCoordinator(
      Config config, EntityCache cache, CacheInvalidationVersionService versionService) {
    this.cache = cache;
    this.versionService = versionService;
    this.pollIntervalMs = config.get(Configs.CACHE_INVALIDATION_POLL_INTERVAL_MS);
    this.enabled = config.get(Configs.CACHE_INVALIDATION_ENABLED);
  }

  /** Starts the background poller. */
  public void start() {
    if (!enabled || !started.compareAndSet(false, true)) {
      return;
    }

    this.localVersion = versionService.currentVersion();
    this.scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "cache-invalidation-poller");
              t.setDaemon(true);
              return t;
            });
    scheduler.scheduleWithFixedDelay(
        this::poll, pollIntervalMs, pollIntervalMs, TimeUnit.MILLISECONDS);
  }

  /** Records a successful write and bumps the global version. */
  public void onWriteSuccess() {
    if (!enabled) {
      return;
    }
    long newVersion = versionService.bumpVersion();
    localVersion = newVersion;
    cache.clear();
  }

  private void poll() {
    try {
      long version = versionService.currentVersion();
      if (version > localVersion) {
        LOG.info(
            "Cache invalidation triggered by remote version change: local={}, remote={}",
            localVersion,
            version);
        cache.clear();
        localVersion = version;
      }
    } catch (Exception e) {
      LOG.warn("Failed to poll cache invalidation version, will retry on next interval.", e);
    }
  }

  @Override
  public void close() {
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }
}
