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
package org.apache.gravitino.catalog.lakehouse.paimon.ops;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.lang.reflect.Field;
import java.util.IdentityHashMap;
import lombok.Getter;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.paimon.catalog.Catalog;

@Getter
public class PaimonBackendCatalogWrapper {

  @VisibleForTesting private final Catalog catalog;
  private final Closeable closeableResource;

  public PaimonBackendCatalogWrapper(Catalog catalog, Closeable closeableResource) {
    this.catalog = catalog;
    this.closeableResource = closeableResource;
  }

  public void close() throws Exception {
    if (catalog != null) {
      catalog.close();
    }
    if (closeableResource != null) {
      closeableResource.close();
    }

    clearClassLoaderReferences();

    Field statisticsCleanerField =
        FieldUtils.getField(FileSystem.Statistics.class, "STATS_DATA_CLEANER", true);
    Object statisticsCleaner = statisticsCleanerField.get(null);
    if (statisticsCleaner != null) {
      ((Thread) statisticsCleaner).interrupt();
      ((Thread) statisticsCleaner).setContextClassLoader(null);
      ((Thread) statisticsCleaner).join();
    }

    FileSystem.closeAll();
  }

  public static void clearClassLoaderReferences() {
    try {
      Class<?> shutdownHooks = Class.forName("java.lang.ApplicationShutdownHooks");
      Field hooksField = shutdownHooks.getDeclaredField("hooks");
      hooksField.setAccessible(true);

      IdentityHashMap<Thread, Thread> hooks =
          (IdentityHashMap<Thread, Thread>) hooksField.get(null);

      hooks
          .entrySet()
          .removeIf(
              entry -> {
                Thread thread = entry.getKey();
                return thread.getContextClassLoader()
                    == PaimonBackendCatalogWrapper.class.getClassLoader();
              });
    } catch (Exception e) {
      throw new RuntimeException("Failed to clean shutdown hooks", e);
    }
  }
}
