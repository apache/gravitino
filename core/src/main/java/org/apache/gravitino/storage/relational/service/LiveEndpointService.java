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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.storage.relational.RelationalEntityStoreIdResolver;
import org.apache.gravitino.storage.relational.mapper.LiveEndpointMapper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/**
 * Fences metadata-object endpoints while a relation write is in progress.
 *
 * <p>Statistic writes and deletes also take this fence, so a statistic cannot be committed against
 * a target that a concurrent drop has already removed.
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
    lockLiveEndpoints(List.of(new Endpoint(identifier, type, observed)));
  }

  /** A name and its stable identity observed while planning a relation write. */
  public record Endpoint(
      NameIdentifier identifier, Entity.EntityType type, NamespacedEntityId ids) {}

  record Row(int depth, Entity.EntityType type, long id) {}

  /** Locks the union of the endpoint chains in hierarchy and ID order and checks every identity. */
  public static void lockLiveEndpoints(Collection<Endpoint> endpoints) {
    Preconditions.checkState(
        SessionUtils.isInTransaction(), "A transaction is required to lock a live endpoint");
    var rows =
        new TreeMap<Row, NameIdentifier>(
            Comparator.comparingInt(Row::depth)
                .thenComparing(Row::type)
                .thenComparingLong(Row::id));
    for (Endpoint endpoint : endpoints) {
      var type = endpoint.type();
      long[] parents = endpoint.ids().namespaceIds();
      if (type != Entity.EntityType.METALAKE) {
        checkNamespaceDepth(type, parents, 1);
        rows.put(new Row(0, Entity.EntityType.METALAKE, parents[0]), endpoint.identifier());
      }
      if (parents.length > 1) {
        rows.put(new Row(1, Entity.EntityType.CATALOG, parents[1]), endpoint.identifier());
        if (type != Entity.EntityType.SCHEMA) {
          checkNamespaceDepth(type, parents, 3);
          rows.put(new Row(2, Entity.EntityType.SCHEMA, parents[2]), endpoint.identifier());
        }
      }
      if (type == Entity.EntityType.COLUMN) {
        checkNamespaceDepth(type, parents, 4);
        rows.put(new Row(3, Entity.EntityType.TABLE, parents[3]), endpoint.identifier());
      } else {
        rows.put(new Row(parents.length, type, endpoint.ids().entityId()), endpoint.identifier());
      }
    }
    rows.forEach((row, name) -> lock(name, row.type(), row.id()));
    SessionUtils.clearSessionCache();
    var resolver = new RelationalEntityStoreIdResolver();
    for (Endpoint endpoint : endpoints) {
      var current = resolver.getEntityIds(endpoint.identifier(), endpoint.type());
      if (!Arrays.equals(current.fullIds(), endpoint.ids().fullIds())) {
        throw missing(endpoint.identifier(), endpoint.type());
      }
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
