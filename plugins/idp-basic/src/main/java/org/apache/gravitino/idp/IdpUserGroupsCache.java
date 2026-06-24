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

package org.apache.gravitino.idp;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.cache.CaffeineGravitinoCache;
import org.apache.gravitino.cache.GravitinoCache;

/** Caches built-in IdP group names keyed by username. */
public class IdpUserGroupsCache {

  private final GravitinoCache<String, List<String>> groupNamesCache;

  /**
   * Creates a user-groups cache using Basic authentication cache settings from the server config.
   *
   * @param config The server configuration.
   */
  public IdpUserGroupsCache(Config config) {
    long expirationSecs = config.get(Configs.BASIC_AUTHENTICATION_GROUPS_CACHE_TTL_SECS);
    long maxSize = config.get(Configs.BASIC_AUTHENTICATION_GROUPS_CACHE_SIZE);
    this.groupNamesCache =
        new CaffeineGravitinoCache<>(TimeUnit.SECONDS.toMillis(expirationSecs), maxSize);
  }

  /**
   * Returns cached group names for the given username, loading them when absent.
   *
   * @param username The username.
   * @param loader Supplies the group names on cache miss.
   * @return The group names for the user.
   */
  public List<String> get(String username, Supplier<List<String>> loader) {
    return groupNamesCache.get(username, ignored -> loader.get());
  }

  /**
   * Invalidates cached group names for the given username.
   *
   * @param username The username whose cached group names should be removed.
   */
  public void invalidate(String username) {
    invalidate(Collections.singleton(username));
  }

  /**
   * Invalidates cached group names for the given usernames.
   *
   * @param usernames The usernames whose cached group names should be removed.
   */
  public void invalidate(Collection<String> usernames) {
    if (usernames == null || usernames.isEmpty()) {
      return;
    }
    groupNamesCache.runInvalidationBatch(
        () -> {
          for (String username : usernames) {
            if (username != null) {
              groupNamesCache.invalidate(username);
            }
          }
        });
  }
}
