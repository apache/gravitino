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

import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

/**
 * Cache configuration class, inheriting from Config. This class defines configuration entries
 * related to caching, including whether to use a weighted cache, cache size limits, and cache
 * expiration settings.
 */
public class CacheConfig extends Config {

  // Maximum number of entries in the cache
  public static final ConfigEntry<Integer> CACHE_MAX_SIZE =
      new ConfigBuilder("gravitino.server.cache.max.size")
          .doc("The max size of the cache in number of entries.")
          .version(ConfigConstants.VERSION_0_10_0)
          .intConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(10_000);

  // Whether to enable cache expiration
  public static final ConfigEntry<Boolean> CACHE_EXPIRATION_ENABLED =
      new ConfigBuilder("gravitino.server.cache.expiration.enabled")
          .doc("Whether to enable cache expiration.")
          .version(ConfigConstants.VERSION_0_10_0)
          .booleanConf()
          .createWithDefault(true);

  // Cache entry expiration time
  public static final ConfigEntry<Long> CACHE_EXPIRATION_TIME =
      new ConfigBuilder("gravitino.server.cache.expiration.time")
          .doc("The time after which cache entries expire. default is 60 minutes.")
          .version(ConfigConstants.VERSION_0_10_0)
          .longConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(60L);

  // Whether to enable cache status
  public static final ConfigEntry<Boolean> CACHE_STATUS_ENABLED =
      new ConfigBuilder("gravitino.server.cache.status.log.enabled")
          .doc(
              "Whether to collect and log cache status. if enabled, cache status will be collected and logged.")
          .version(ConfigConstants.VERSION_0_10_0)
          .booleanConf()
          .createWithDefault(false);

  // Whether to enable weighted cache
  public static final ConfigEntry<Boolean> CACHE_WEIGHER_ENABLED =
      new ConfigBuilder("gravitino.server.cache.weigher.enabled")
          .doc(
              "Whether to enable weighted cache. if enabled, cache entries will be weighed based on their weight, not"
                  + " size.")
          .version(ConfigConstants.VERSION_0_10_0)
          .booleanConf()
          .createWithDefault(true);

  // Maximum weight of cache entries
  public static final ConfigEntry<Long> CACHE_MAX_WEIGHT =
      new ConfigBuilder("gravitino.server.cache.max.weight")
          .doc("The maximum weight of cache entries. default is 10000.")
          .version(ConfigConstants.VERSION_0_10_0)
          .longConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(EntityCacheWeigher.getMaxWeight());

  // Time unit for cache expiration
  private static final TimeUnit expirationTimeUnit = TimeUnit.MINUTES;

  /**
   * Constructs a new CacheConfig instance.
   *
   * @param loadDefaults whether to load the default configuration
   */
  public CacheConfig(boolean loadDefaults) {
    super(loadDefaults);
  }

  /** Default constructor, loads the default configuration. */
  public CacheConfig() {
    this(true);
  }

  /**
   * Gets the maximum cache size
   *
   * @return the maximum cache size
   */
  public int getMaxSize() {
    return get(CACHE_MAX_SIZE);
  }

  /**
   * Checks if cache expiration is enabled
   *
   * @return {@code true} if cache expiration is enabled, {@code false} otherwise
   */
  public boolean isExpirationEnabled() {
    return get(CACHE_EXPIRATION_ENABLED);
  }

  /**
   * Gets the cache expiration time
   *
   * @return the cache expiration time
   */
  public long getExpirationTime() {
    return get(CACHE_EXPIRATION_TIME);
  }

  /**
   * Gets the time unit for cache expiration
   *
   * @return A {@link TimeUnit} representing the time unit for cache expiration.
   */
  public TimeUnit getExpirationTimeUnit() {
    return expirationTimeUnit;
  }

  /**
   * Checks if cache status is enabled
   *
   * @return {@code true} if cache status is enabled, {@code false} otherwise
   */
  public boolean isCacheStatusEnabled() {
    return get(CACHE_STATUS_ENABLED);
  }

  /**
   * Checks if weighted cache is enabled
   *
   * @return {@code true} if weighted cache is enabled, {@code false} otherwise
   */
  public boolean isWeigherEnabled() {
    return get(CACHE_WEIGHER_ENABLED);
  }

  /**
   * Gets the maximum weight of cache entries
   *
   * @return the maximum weight of cache entries
   */
  public long getMaxWeight() {
    return get(CACHE_MAX_WEIGHT);
  }
}
