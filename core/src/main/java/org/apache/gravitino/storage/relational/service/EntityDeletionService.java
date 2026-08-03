/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.storage.relational.service;

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.mapper.EntityDeletionMapper;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** Storage service for active metadata deletion generations. */
public class EntityDeletionService {

  private static final EntityDeletionService INSTANCE = new EntityDeletionService();

  /**
   * Returns the singleton deletion storage service.
   *
   * @return deletion storage service
   */
  public static EntityDeletionService getInstance() {
    return INSTANCE;
  }

  private EntityDeletionService() {}

  /** Inserts one deletion generation in the caller's transaction. */
  void insertWithoutCommit(EntityDeletionPO deletion) {
    Objects.requireNonNull(deletion, "deletion must not be null");
    SessionUtils.doWithoutCommit(
        EntityDeletionMapper.class, mapper -> mapper.insertEntityDeletion(deletion));
  }

  /**
   * Loads one exact deletion generation.
   *
   * <p>This read also joins an existing outer session when called from a larger relational
   * transaction.
   *
   * @param deletionId opaque deletion identifier
   * @return deletion generation, or {@code null} when absent
   */
  @Nullable
  public EntityDeletionPO get(String deletionId) {
    Objects.requireNonNull(deletionId, "deletionId must not be null");
    return SessionUtils.doWithCommitAndFetchResult(
        EntityDeletionMapper.class, mapper -> mapper.selectEntityDeletion(deletionId));
  }

  /**
   * Loads and locks one exact action in the caller's transaction.
   *
   * @param deletionId opaque deletion identifier
   * @return deletion action, or {@code null} when absent
   */
  @Nullable
  EntityDeletionPO getForUpdate(String deletionId) {
    Objects.requireNonNull(deletionId, "deletionId must not be null");
    return SessionUtils.getWithoutCommit(
        EntityDeletionMapper.class, mapper -> mapper.selectEntityDeletionForUpdate(deletionId));
  }

  /**
   * Deletes an exact action after the caller has locked and validated it.
   *
   * @param deletionId opaque deletion identifier
   * @return {@code true} when one row was deleted
   */
  boolean delete(String deletionId) {
    Objects.requireNonNull(deletionId, "deletionId must not be null");
    return SessionUtils.getWithoutCommit(
            EntityDeletionMapper.class, mapper -> mapper.deleteEntityDeletion(deletionId))
        == 1;
  }
}
