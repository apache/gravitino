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

package org.apache.gravitino.storage.relational;

import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.apache.gravitino.Configs.VERSION_RETENTION_COUNT;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RelationalGarbageCollector implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(RelationalGarbageCollector.class);
  private final RelationalBackend backend;

  private final long storeDeleteAfterTimeMillis;
  private final long versionRetentionCount;

  @VisibleForTesting
  final ScheduledExecutorService garbageCollectorPool =
      new ScheduledThreadPoolExecutor(
          2,
          r -> {
            Thread t = new Thread(r, "RelationalBackend-Garbage-Collector");
            t.setDaemon(true);
            return t;
          },
          new ThreadPoolExecutor.AbortPolicy());

  public RelationalGarbageCollector(RelationalBackend backend, Config config) {
    this.backend = backend;
    storeDeleteAfterTimeMillis = config.get(STORE_DELETE_AFTER_TIME);
    versionRetentionCount = config.get(VERSION_RETENTION_COUNT);
  }

  public void start() {
    long dateTimelineMinute = storeDeleteAfterTimeMillis / 1000 / 60;

    // We will collect garbage every 10 minutes at least. If the dateTimelineMinute is larger than
    // 100 minutes, we would collect garbage every dateTimelineMinute/10 minutes.
    long frequency = Math.max(dateTimelineMinute / 10, 10);
    garbageCollectorPool.scheduleAtFixedRate(this::collectAndClean, 5, frequency, TimeUnit.MINUTES);
  }

  @VisibleForTesting
  public void collectAndClean() {
    long threadId = Thread.currentThread().getId();
    LOG.info("Thread {} start to collect garbage...", threadId);

    try {
      LOG.info("Start to collect and delete legacy data by thread {}", threadId);
      long legacyTimeline = System.currentTimeMillis() - storeDeleteAfterTimeMillis;
      for (Entity.EntityType entityType : Entity.EntityType.values()) {
        long deletedCount = Long.MAX_VALUE;
        LOG.info(
            "Try to physically delete {} legacy data that has been marked deleted before {}",
            entityType,
            legacyTimeline);
        try {
          while (deletedCount > 0) {
            deletedCount = backend.hardDeleteLegacyData(entityType, legacyTimeline);
          }
        } catch (RuntimeException e) {
          LOG.error("Failed to physically delete type of " + entityType + "'s legacy data: ", e);
        }
      }

      LOG.info("Start to collect and delete old version data by thread {}", threadId);
      for (Entity.EntityType entityType : Entity.EntityType.values()) {
        long deletedCount = Long.MAX_VALUE;
        LOG.info(
            "Try to softly delete {} old version data that has been over retention count {}",
            entityType,
            versionRetentionCount);
        try {
          while (deletedCount > 0) {
            deletedCount = backend.deleteOldVersionData(entityType, versionRetentionCount);
          }
        } catch (RuntimeException e) {
          LOG.error("Failed to softly delete type of " + entityType + "'s old version data: ", e);
        }
      }
    } catch (Exception e) {
      LOG.error("Thread {} failed to collect and clean garbage.", threadId, e);
    } finally {
      LOG.info("Thread {} finish to collect garbage.", threadId);
    }
  }

  @Override
  public void close() throws IOException {
    this.garbageCollectorPool.shutdown();
    try {
      if (!this.garbageCollectorPool.awaitTermination(5, TimeUnit.SECONDS)) {
        this.garbageCollectorPool.shutdownNow();
      }
    } catch (InterruptedException ex) {
      this.garbageCollectorPool.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
