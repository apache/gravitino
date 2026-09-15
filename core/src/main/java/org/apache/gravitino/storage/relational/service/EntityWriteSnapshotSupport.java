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

import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.UnsupportedEntityTypeException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ViewMetaMapper;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** Row-lock support for reconciling external catalog registrations. */
public final class EntityWriteSnapshotSupport {
  private EntityWriteSnapshotSupport() {}

  /**
   * Locks a live registration and returns its storage version. The caller must own a transaction.
   *
   * @param type the registration type
   * @param id the stable registration ID
   * @return the current storage version
   */
  public static long lockVersion(Entity.EntityType type, long id) {
    if (!SessionUtils.isInTransaction()) {
      throw new IllegalStateException("A write snapshot requires an active transaction");
    }
    clearReadCache();
    try {
      switch (type) {
        case SCHEMA:
          return readVersion(
              SchemaMetaMapper.class,
              mapper ->
                  version(mapper.selectSchemaMetaByIdForUpdate(id), po -> po.getCurrentVersion()));
        case TABLE:
          return readVersion(
              TableMetaMapper.class,
              mapper ->
                  version(mapper.selectTableMetaByIdForUpdate(id), po -> po.getCurrentVersion()));
        case TOPIC:
          return readVersion(
              TopicMetaMapper.class,
              mapper ->
                  version(mapper.selectTopicMetaByIdForUpdate(id), po -> po.getCurrentVersion()));
        case VIEW:
          return readVersion(
              ViewMetaMapper.class,
              mapper ->
                  version(mapper.selectViewMetaByIdForUpdate(id), po -> po.getCurrentVersion()));
        default:
          throw new UnsupportedEntityTypeException(
              "Write snapshots are supported for external schema, table, topic and view registrations, not %s",
              type);
      }
    } finally {
      // A name lookup before waiting for the row lock may now be stale in MyBatis.
      clearReadCache();
    }
  }

  private static void clearReadCache() {
    try {
      SqlSessions.getSqlSession().clearCache();
    } finally {
      SqlSessions.closeSqlSession();
    }
  }

  private static <M> long readVersion(Class<M> mapper, Function<M, Long> read) {
    Long version = SessionUtils.getWithoutCommit(mapper, read);
    if (version == null) {
      throw new NoSuchEntityException("The observed registration no longer exists");
    }
    return version;
  }

  private static <P> Long version(P po, Function<P, Long> version) {
    return po == null ? null : version.apply(po);
  }
}
