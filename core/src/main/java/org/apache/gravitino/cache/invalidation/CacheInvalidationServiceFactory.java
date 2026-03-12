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

package org.apache.gravitino.cache.invalidation;

import com.google.common.annotations.VisibleForTesting;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;

/** Singleton factory for cache invalidation service. */
public final class CacheInvalidationServiceFactory {
  private static volatile CacheInvalidationService invalidationService;

  private CacheInvalidationServiceFactory() {}

  public static CacheInvalidationService getOrCreate(Config config) {
    if (invalidationService == null) {
      synchronized (CacheInvalidationServiceFactory.class) {
        if (invalidationService == null) {
          invalidationService =
              new LocalCacheInvalidationService(config.get(Configs.CACHE_INVALIDATION_ENABLED));
        }
      }
    }
    return invalidationService;
  }

  @VisibleForTesting
  public static void resetForTest() {
    synchronized (CacheInvalidationServiceFactory.class) {
      invalidationService = null;
    }
  }
}
