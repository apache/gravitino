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
package org.apache.gravitino.hive;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

public class ClientPropertiesMetadata implements PropertiesMetadata {
  private static final int DEFAULT_CLIENT_POOL_SIZE = 1;
  private static final long DEFAULT_CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS =
      TimeUnit.MINUTES.toMillis(5);
  private static final Map<String, PropertyEntry<?>> PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              HiveConstants.CLIENT_POOL_SIZE,
              PropertyEntry.integerOptionalPropertyEntry(
                  HiveConstants.CLIENT_POOL_SIZE,
                  "The maximum number of Hive metastore clients in the pool for Gravitino",
                  false /* immutable */,
                  DEFAULT_CLIENT_POOL_SIZE,
                  false /* hidden */))
          .put(
              HiveConstants.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
              PropertyEntry.longOptionalPropertyEntry(
                  HiveConstants.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                  "The cache pool eviction interval",
                  false /* immutable */,
                  DEFAULT_CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                  false /* hidden */))
          .build();

  @Override
  public Map<String, PropertyEntry<?>> propertyEntries() {
    return PROPERTY_ENTRIES;
  }
}
