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

import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

public class CacheConfig extends Config {

  public static final ConfigEntry<Boolean> USE_WEIGHTED_CACHE =
      new ConfigBuilder("gravitino.server.cache.weighted.enabled")
          .doc("Whether to use a weighted cache.")
          .version(ConfigConstants.VERSION_0_9_0)
          .booleanConf()
          .createWithDefault(false);

  public static final ConfigEntry<Long> ENTITY_CACHE_WEIGHER_TARGET =
      new ConfigBuilder("gravitino.server.cache.weighted.target")
          .doc("The target weight of the cache.")
          .version(ConfigConstants.VERSION_0_9_0)
          .longConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(100 * MetadataEntityWeigher.WEIGHT_PER_MB);
  public static final ConfigEntry<Integer> CACHE_MAX_SIZE =
      new ConfigBuilder("gravitino.server.cache.max.size")
          .doc("The max size of the cache in number of entries.")
          .version(ConfigConstants.VERSION_0_9_0)
          .intConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(10_000);

  public static final ConfigEntry<Boolean> CACHE_EXPIRATION_ENABLED =
      new ConfigBuilder("gravitino.server.cache.expiration.enabled")
          .doc("Whether to enable cache expiration.")
          .version(ConfigConstants.VERSION_0_9_0)
          .booleanConf()
          .createWithDefault(true);

  public static final ConfigEntry<Long> CACHE_EXPIRATION_TIME =
      new ConfigBuilder("gravitino.server.cache.expiration.time")
          .doc("The time after which cache entries expire.")
          .version(ConfigConstants.VERSION_0_9_0)
          .longConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(60L);

  private static final TimeUnit expirationTimeUnit = TimeUnit.MINUTES;

  public CacheConfig(boolean loadDefaults) {
    super(loadDefaults);
  }

  public CacheConfig() {
    this(true);
  }

  public int getMaxSize() {
    return get(CACHE_MAX_SIZE);
  }

  public boolean isExpirationEnabled() {
    return get(CACHE_EXPIRATION_ENABLED);
  }

  public long getExpirationTime() {
    return get(CACHE_EXPIRATION_TIME);
  }

  public TimeUnit getExpirationTimeUnit() {
    return expirationTimeUnit;
  }

  public boolean isWeightedCacheEnabled() {
    return get(USE_WEIGHTED_CACHE);
  }

  public long getEntityCacheWeigherTarget() {
    return get(ENTITY_CACHE_WEIGHER_TARGET);
  }
}
