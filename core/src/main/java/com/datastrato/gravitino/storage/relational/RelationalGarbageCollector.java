/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational;

import static com.datastrato.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static com.datastrato.gravitino.Configs.VERSION_RETENTION_COUNT;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Entity;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
    long dateTimeLineMinute = storeDeleteAfterTimeMillis / 1000 / 60;

    // We will collect garbage every 10 minutes at least. If the dateTimeLineMinute is larger than
    // 100 minutes, we would collect garbage every dateTimeLineMinute/10 minutes.
    long frequency = Math.max(dateTimeLineMinute / 10, 10);
    garbageCollectorPool.scheduleAtFixedRate(this::collectAndClean, 5, frequency, TimeUnit.MINUTES);
  }

  private void collectAndClean() {
    long threadId = Thread.currentThread().getId();
    LOG.info("Thread {} start to collect garbage...", threadId);

    try {
      LOG.info("Start to collect and delete legacy data by thread {}", threadId);
      long legacyTimeLine = System.currentTimeMillis() - storeDeleteAfterTimeMillis;
      for (Entity.EntityType entityType : Entity.EntityType.values()) {
        long deletedCount = Long.MAX_VALUE;
        while (deletedCount > 0) {
          deletedCount = backend.hardDeleteLegacyData(entityType, legacyTimeLine);
        }
      }

      LOG.info("Start to collect and delete old version data by thread {}", threadId);
      for (Entity.EntityType entityType : Entity.EntityType.values()) {
        long deletedCount = Long.MAX_VALUE;
        while (deletedCount > 0) {
          deletedCount = backend.deleteOldVersionData(entityType, versionRetentionCount);
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
