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
import java.util.Arrays;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.storage.relational.RelationalEntityStoreIdResolver;
import org.apache.gravitino.storage.relational.mapper.LiveEndpointMapper;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.session.SqlSession;

/**
 * Fences metadata-object endpoints while a relation write is in progress.
 *
 * <p>Statistic writers take a schema lock of their own today and adopt this fence in #13177, so
 * nothing here covers them yet.
 */
public final class LiveEndpointService {

  private LiveEndpointService() {}

  /**
   * Locks the observed endpoint and its ancestors in hierarchy order, then verifies that the name
   * still resolves to the same ID chain. The caller must keep the current transaction open until
   * its dependent write completes. For a column, the table lock also fences column changes.
   *
   * @param identifier the originally observed endpoint name
   * @param type the endpoint type
   * @param observed the originally observed endpoint and namespace IDs
   */
  public static void lockLiveEndpoint(
      NameIdentifier identifier, Entity.EntityType type, NamespacedEntityId observed) {
    Preconditions.checkState(
        SessionUtils.isInTransaction(), "A transaction is required to lock a live endpoint");
    long[] namespaceIds = observed.namespaceIds();
    if (type == Entity.EntityType.METALAKE) {
      lock(identifier, type, observed.entityId());
    } else {
      checkNamespaceDepth(type, namespaceIds, 1);
      lock(identifier, Entity.EntityType.METALAKE, namespaceIds[0]);
      if (type == Entity.EntityType.CATALOG) {
        lock(identifier, type, observed.entityId());
      } else if (namespaceIds.length > 1) {
        lock(identifier, Entity.EntityType.CATALOG, namespaceIds[1]);
        if (type == Entity.EntityType.SCHEMA) {
          lock(identifier, type, observed.entityId());
        } else {
          checkNamespaceDepth(type, namespaceIds, 3);
          lock(identifier, Entity.EntityType.SCHEMA, namespaceIds[2]);
          if (type == Entity.EntityType.COLUMN) {
            checkNamespaceDepth(type, namespaceIds, 4);
            lock(identifier, Entity.EntityType.TABLE, namespaceIds[3]);
          } else {
            lock(identifier, type, observed.entityId());
          }
        }
      } else {
        lock(identifier, type, observed.entityId());
      }
    }

    // An earlier name lookup in this transaction may be in MyBatis's first-level cache. The
    // locking reads above must be followed by a database read of the current name-to-ID chain.
    SqlSession session = SqlSessions.getSqlSession();
    try {
      session.clearCache();
    } finally {
      SqlSessions.closeSqlSession();
    }
    NamespacedEntityId current =
        new RelationalEntityStoreIdResolver().getEntityIds(identifier, type);
    if (!Arrays.equals(current.fullIds(), observed.fullIds())) {
      throw missing(identifier, type);
    }
  }

  /**
   * Rejects a namespace that is too shallow for the walk below it.
   *
   * <p>The walk indexes the namespace by position, so a chain shorter than its type implies would
   * otherwise leave the endpoint partly unlocked or fail with an index error that names nothing.
   *
   * @param type the endpoint type being locked
   * @param namespaceIds the observed namespace ID chain
   * @param required the number of namespace IDs the walk is about to rely on
   */
  private static void checkNamespaceDepth(
      Entity.EntityType type, long[] namespaceIds, int required) {
    Preconditions.checkArgument(
        namespaceIds.length >= required,
        "A %s endpoint needs at least %s namespace IDs, got %s",
        type,
        required,
        namespaceIds.length);
  }

  private static void lock(NameIdentifier identifier, Entity.EntityType type, long id) {
    Long locked =
        SessionUtils.getWithoutCommit(
            LiveEndpointMapper.class, mapper -> mapper.lockLiveEndpoint(type, id));
    if (locked == null) {
      throw missing(identifier, type);
    }
  }

  private static NoSuchEntityException missing(NameIdentifier identifier, Entity.EntityType type) {
    return new NoSuchEntityException(
        NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, type.name().toLowerCase(), identifier);
  }
}
