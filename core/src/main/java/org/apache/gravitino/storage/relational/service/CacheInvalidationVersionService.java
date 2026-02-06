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

package org.apache.gravitino.storage.relational.service;

import com.google.common.base.Preconditions;
import org.apache.gravitino.storage.relational.mapper.CacheInvalidationVersionMapper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Service that manages the cache invalidation version stored in the database. */
public class CacheInvalidationVersionService {

  private static final Logger LOG = LoggerFactory.getLogger(CacheInvalidationVersionService.class);

  private static final CacheInvalidationVersionService INSTANCE =
      new CacheInvalidationVersionService();

  private static final long DEFAULT_ROW_ID = 1L;

  public static CacheInvalidationVersionService getInstance() {
    return INSTANCE;
  }

  private CacheInvalidationVersionService() {}

  /**
   * Returns the current version, initializing the row if it is missing.
   *
   * @return the current version value
   */
  public long currentVersion() {
    return SessionUtils.doWithCommitAndFetchResult(
        CacheInvalidationVersionMapper.class,
        mapper -> {
          Long version = mapper.selectVersion(DEFAULT_ROW_ID);
          if (version != null) {
            return version;
          }

          long now = System.currentTimeMillis();
          try {
            mapper.insertVersion(DEFAULT_ROW_ID, 0L, now);
          } catch (Exception e) {
            LOG.debug("Version row already exists, continue to read it", e);
          }

          Long inserted = mapper.selectVersion(DEFAULT_ROW_ID);
          Preconditions.checkState(
              inserted != null, "Failed to initialize cache invalidation version row.");
          return inserted;
        });
  }

  /**
   * Increments the version and returns the latest value.
   *
   * @return incremented version value
   */
  public long bumpVersion() {
    return SessionUtils.doWithCommitAndFetchResult(
        CacheInvalidationVersionMapper.class,
        mapper -> {
          long now = System.currentTimeMillis();
          int updated = mapper.incrementVersion(DEFAULT_ROW_ID, now);
          if (updated == 0) {
            try {
              mapper.insertVersion(DEFAULT_ROW_ID, 0L, now);
            } catch (RuntimeException e) {
              LOG.debug("Failed to insert version row, it may already exist: {}", e.getMessage());
            }
            mapper.incrementVersion(DEFAULT_ROW_ID, now);
          }
          Long version = mapper.selectVersion(DEFAULT_ROW_ID);
          Preconditions.checkState(
              version != null, "Failed to read cache invalidation version after bump.");
          return version;
        });
  }
}
