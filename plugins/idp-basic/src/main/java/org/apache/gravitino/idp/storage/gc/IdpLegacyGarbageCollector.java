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
package org.apache.gravitino.idp.storage.gc;

import static org.apache.gravitino.Configs.GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Config;
import org.apache.gravitino.idp.storage.service.IdpBasicGroupMetaService;
import org.apache.gravitino.idp.storage.service.IdpBasicUserMetaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Periodically hard-deletes legacy built-in IdP metadata without core module hooks. */
public final class IdpLegacyGarbageCollector implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(IdpLegacyGarbageCollector.class);

  private final long storeDeleteAfterTimeMillis;
  private final IdpBasicUserMetaService userMetaService = new IdpBasicUserMetaService();
  private final IdpBasicGroupMetaService groupMetaService = new IdpBasicGroupMetaService();

  @VisibleForTesting
  final ScheduledExecutorService garbageCollectorPool =
      new ScheduledThreadPoolExecutor(
          1,
          r -> {
            Thread t = new Thread(r, "IdpBasic-Legacy-Garbage-Collector");
            t.setDaemon(true);
            return t;
          },
          new ThreadPoolExecutor.AbortPolicy());

  /**
   * Creates a collector using the same retention configuration as the relational store GC.
   *
   * @param config server configuration
   */
  public IdpLegacyGarbageCollector(Config config) {
    this.storeDeleteAfterTimeMillis = config.get(STORE_DELETE_AFTER_TIME);
  }

  /** Starts the periodic garbage collection task. */
  public void start() {
    long dateTimelineMinute = storeDeleteAfterTimeMillis / 1000 / 60;
    long frequency = Math.max(dateTimelineMinute / 10, 10);
    garbageCollectorPool.scheduleAtFixedRate(this::collectAndClean, 5, frequency, TimeUnit.MINUTES);
    LOG.info("Started built-in IdP legacy metadata garbage collector");
  }

  @VisibleForTesting
  void collectAndClean() {
    long threadId = Thread.currentThread().getId();
    LOG.debug("Thread {} start to collect built-in IdP garbage...", threadId);

    try {
      long legacyTimeline = System.currentTimeMillis() - storeDeleteAfterTimeMillis;
      collectLegacyData(
          "idp_user", legacyTimeline, userMetaService::deleteUserMetasByLegacyTimeline);
      collectLegacyData(
          "idp_group", legacyTimeline, groupMetaService::deleteGroupMetasByLegacyTimeline);
    } catch (Exception e) {
      LOG.error("Thread {} failed to collect and clean built-in IdP garbage.", threadId, e);
    } finally {
      LOG.debug("Thread {} finish to collect built-in IdP garbage.", threadId);
    }
  }

  private void collectLegacyData(String entityLabel, long legacyTimeline, LegacyDeletion deletion) {
    long deletedCount = Long.MAX_VALUE;
    LOG.debug(
        "Try to physically delete {} legacy data that has been marked deleted before {}",
        entityLabel,
        legacyTimeline);
    try {
      while (deletedCount > 0) {
        deletedCount = deletion.delete(legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      }
    } catch (RuntimeException e) {
      LOG.error("Failed to physically delete {} legacy data: ", entityLabel, e);
    }
  }

  @Override
  public void close() throws IOException {
    garbageCollectorPool.shutdown();
    try {
      if (!garbageCollectorPool.awaitTermination(5, TimeUnit.SECONDS)) {
        garbageCollectorPool.shutdownNow();
      }
    } catch (InterruptedException ex) {
      garbageCollectorPool.shutdownNow();
      Thread.currentThread().interrupt();
    }
    LOG.info("Stopped built-in IdP legacy metadata garbage collector");
  }

  @FunctionalInterface
  private interface LegacyDeletion {
    int delete(long legacyTimeline, int limit);
  }
}
