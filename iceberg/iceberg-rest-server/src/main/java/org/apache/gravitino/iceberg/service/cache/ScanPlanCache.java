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

package org.apache.gravitino.iceberg.service.cache;

import java.io.Closeable;
import java.util.Optional;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;

/** Interface for caching scan plans. */
public interface ScanPlanCache extends Closeable {

  /**
   * Retrieves a cached scan plan response for the given key.
   *
   * @param key the cache key containing table identifier and scan parameters
   * @return an Optional containing the cached scan plan response, or empty if not found
   */
  Optional<PlanTableScanResponse> get(ScanPlanCacheKey key);

  /**
   * Stores a scan plan response in the cache.
   *
   * @param key the cache key containing table identifier and scan parameters
   * @param scanResponse the scan plan response to cache
   */
  void put(ScanPlanCacheKey key, PlanTableScanResponse scanResponse);
}
