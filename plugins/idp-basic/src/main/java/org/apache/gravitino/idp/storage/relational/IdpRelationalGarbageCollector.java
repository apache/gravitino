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
package org.apache.gravitino.idp.storage.relational;

import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Config;
import org.apache.gravitino.idp.meta.IdpEntityType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Garbage collector for built-in IdP relational entities. */
public final class IdpRelationalGarbageCollector implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(IdpRelationalGarbageCollector.class);

  private static final List<IdpEntityType> IDP_ENTITY_TYPES =
      ImmutableList.of(IdpEntityType.IDP_USER, IdpEntityType.IDP_GROUP);

  private final IdpJDBCBackend backend;
  private final long storeDeleteAfterTimeMillis;

  @VisibleForTesting
  final ScheduledExecutorService garbageCollectorPool =
      new ScheduledThreadPoolExecutor(
          2,
          r -> {
            Thread t = new Thread(r, "IdpJDBCBackend-Garbage-Collector");
            t.setDaemon(true);
            return t;
          },
          new ThreadPoolExecutor.AbortPolicy());

  /**
   * Creates a garbage collector for built-in IdP relational entities.
   *
   * @param backend The relational backend.
   * @param config The server configuration.
   */
  public IdpRelationalGarbageCollector(IdpJDBCBackend backend, Config config) {
    this.backend = backend;
    storeDeleteAfterTimeMillis = config.get(STORE_DELETE_AFTER_TIME);
  }

  /** Starts the scheduled garbage collector. */
  public void start() {
    long dateTimelineMinute = storeDeleteAfterTimeMillis / 1000 / 60;
    long frequency = Math.max(dateTimelineMinute / 10, 10);
    garbageCollectorPool.scheduleAtFixedRate(this::collectAndClean, 5, frequency, TimeUnit.MINUTES);
  }

  @VisibleForTesting
  void collectAndClean() {
    long threadId = Thread.currentThread().getId();
    LOG.debug("Thread {} start to collect built-in IdP garbage...", threadId);

    try {
      long legacyTimeline = System.currentTimeMillis() - storeDeleteAfterTimeMillis;
      for (IdpEntityType entityType : IDP_ENTITY_TYPES) {
        long deletedCount = Long.MAX_VALUE;
        try {
          while (deletedCount > 0) {
            deletedCount = backend.hardDeleteLegacyData(entityType, legacyTimeline);
          }
        } catch (RuntimeException e) {
          LOG.error("Failed to physically delete {} legacy data: ", entityType, e);
        }
      }
    } catch (Exception e) {
      LOG.error("Thread {} failed to collect and clean built-in IdP garbage.", threadId, e);
    } finally {
      LOG.debug("Thread {} finish to collect built-in IdP garbage.", threadId);
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
  }
}
