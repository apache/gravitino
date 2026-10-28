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
package org.apache.gravitino.connector;

/**
 * Supplies an absolute deadline for a catalog entry in {@code CatalogManager}'s catalog cache.
 *
 * <p>When the returned time is finite, the cache keeps the catalog only until that deadline, and
 * still no longer than the configured idle interval. {@link Long#MAX_VALUE} means the idle interval
 * alone applies.
 */
public interface CatalogCacheDeadline {

  /**
   * Epoch millis when this catalog should leave the catalog cache.
   *
   * @return deadline epoch millis, or {@link Long#MAX_VALUE} when no absolute deadline is known
   */
  long cacheDeadlineEpochMs();
}
