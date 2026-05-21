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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Config;
import org.apache.gravitino.idp.storage.service.IdpGroupMetaService;
import org.apache.gravitino.idp.storage.service.IdpUserMetaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IdpLegacyGarbageCollector implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(IdpLegacyGarbageCollector.class);

  private final long storeDeleteAfterTimeMillis;

  private final ScheduledExecutorService garbageCollectorPool =
      new ScheduledThreadPoolExecutor(
          2,
          r -> {
            Thread t = new Thread(r, "IdpBasic-Legacy-Garbage-Collector");
            t.setDaemon(true);
            return t;
          },
          new ThreadPoolExecutor.AbortPolicy());

  public IdpLegacyGarbageCollector(Config config) {
    storeDeleteAfterTimeMillis = config.get(STORE_DELETE_AFTER_TIME);
  }

  public void start() {
    long dateTimelineMinute = storeDeleteAfterTimeMillis / 1000 / 60;

    // We will collect garbage every 10 minutes at least. If the dateTimelineMinute is larger than
    // 100 minutes, we would collect garbage every dateTimelineMinute/10 minutes.
    long frequency = Math.max(dateTimelineMinute / 10, 10);
    garbageCollectorPool.scheduleAtFixedRate(this::collectAndClean, 5, frequency, TimeUnit.MINUTES);
  }

  public void collectAndClean() {
    long threadId = Thread.currentThread().getId();
    LOG.debug("Thread {} start to collect garbage...", threadId);

    try {
      LOG.debug("Start to collect and delete legacy data by thread {}", threadId);
      long legacyTimeline = System.currentTimeMillis() - storeDeleteAfterTimeMillis;
      long deletedCount = Long.MAX_VALUE;
      LOG.debug(
          "Try to physically delete {} legacy data that has been marked deleted before {}",
          "idp_user",
          legacyTimeline);
      try {
        while (deletedCount > 0) {
          deletedCount =
              IdpUserMetaService.getInstance()
                  .deleteUserMetasByLegacyTimeline(
                      legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
        }
      } catch (RuntimeException e) {
        LOG.error("Failed to physically delete type of idp_user's legacy data: ", e);
      }

      deletedCount = Long.MAX_VALUE;
      LOG.debug(
          "Try to physically delete {} legacy data that has been marked deleted before {}",
          "idp_group",
          legacyTimeline);
      try {
        while (deletedCount > 0) {
          deletedCount =
              IdpGroupMetaService.getInstance()
                  .deleteGroupMetasByLegacyTimeline(
                      legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
        }
      } catch (RuntimeException e) {
        LOG.error("Failed to physically delete type of idp_group's legacy data: ", e);
      }
    } catch (Exception e) {
      LOG.error("Thread {} failed to collect and clean garbage.", threadId, e);
    } finally {
      LOG.debug("Thread {} finish to collect garbage.", threadId);
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
