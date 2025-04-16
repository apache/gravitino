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

package org.apache.gravitino.filesystem.hadoop;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.file.FilesetCatalog;

/** A cache for fileset catalogs. */
public class FilesetCatalogCache implements Closeable {

  private final GravitinoClient client;
  private final Cache<NameIdentifier, FilesetCatalog> catalogCache;

  // Since Caffeine does not ensure that removalListener will be involved after expiration
  // We use a scheduler with one thread to clean up expired clients.
  private final ScheduledThreadPoolExecutor catalogCleanScheduler;

  /**
   * Creates a new instance of {@link FilesetCatalogCache}.
   *
   * @param client the Gravitino client.
   */
  public FilesetCatalogCache(GravitinoClient client) {
    this.client = client;
    this.catalogCleanScheduler =
        new ScheduledThreadPoolExecutor(1, newDaemonThreadFactory("gvfs-catalog-cache-cleaner"));
    this.catalogCache = newCatalogCache(catalogCleanScheduler);
  }

  /**
   * Gets the fileset catalog by the given catalog identifier.
   *
   * @param catalogIdent the catalog identifier.
   * @return the fileset catalog.
   */
  public FilesetCatalog getFilesetCatalog(NameIdentifier catalogIdent) {
    FilesetCatalog filesetCatalog =
        catalogCache.get(
            catalogIdent, ident -> client.loadCatalog(catalogIdent.name()).asFilesetCatalog());

    Preconditions.checkArgument(
        filesetCatalog != null, String.format("Loaded fileset catalog: %s is null.", catalogIdent));
    return filesetCatalog;
  }

  private Cache<NameIdentifier, FilesetCatalog> newCatalogCache(
      ScheduledThreadPoolExecutor catalogCleanScheduler) {
    // In most scenarios, it will not read so many catalog filesets at the same time, so we can just
    // set a default value for this cache.
    return Caffeine.newBuilder()
        .maximumSize(100)
        .scheduler(Scheduler.forScheduledExecutorService(catalogCleanScheduler))
        .build();
  }

  private ThreadFactory newDaemonThreadFactory(String name) {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(name + "-%d").build();
  }

  @Override
  public void close() throws IOException {
    catalogCache.invalidateAll();
    // close the client
    try {
      if (client != null) {
        client.close();
      }
    } catch (Exception e) {
      // ignore
    }
    catalogCleanScheduler.shutdownNow();
  }
}
