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

import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Routes relational PO reads between parent-id based SQL (when the entity-id cache is enabled) and
 * full qualified name SQL. Callers keep cache policy in one place instead of embedding it in {@link
 * BasePOStorageOps}.
 */
public final class POStorageReadRouting {

  private POStorageReadRouting() {}

  /**
   * Loads a PO using parent-id based SQL when the entity-id cache is enabled and {@code ops}
   * supports that path; otherwise uses full qualified name SQL.
   *
   * @param mapper MyBatis mapper session
   * @param identifier entity name identifier (logical naming when wrapped by hierarchical ops)
   * @param ops storage operations delegate
   * @param entityType entity type for {@code identifier} (used to resolve the parent id)
   * @param <PO> persistent object type
   * @param <Mapper> mapper type
   * @return loaded PO or null when the delegate returns null
   */
  public static <PO, Mapper> PO getPO(
      Mapper mapper,
      NameIdentifier identifier,
      BasePOStorageOps<PO, Mapper> ops,
      Entity.EntityType entityType) {
    return getPO(mapper, identifier, ops, entityType, GravitinoEnv.getInstance().cacheEnabled());
  }

  /**
   * Same as {@link #getPO} but uses an explicit cache flag (typically for tests).
   *
   * @param cacheEnabled when true, prefer parent-id based reads when supported
   */
  public static <PO, Mapper> PO getPO(
      Mapper mapper,
      NameIdentifier identifier,
      BasePOStorageOps<PO, Mapper> ops,
      Entity.EntityType entityType,
      boolean cacheEnabled) {
    if (cacheEnabled && ops.supportsParentIdRelationalRead()) {
      Long parentId =
          EntityIdService.getEntityId(
              NameIdentifier.parse(identifier.namespace().toString()),
              NameIdentifierUtil.parentEntityType(entityType));
      return ops.getPO(mapper, parentId, identifier.name());
    }
    return ops.getPOByFullName(mapper, identifier);
  }

  /**
   * Lists POs under a namespace using parent-id based SQL when the entity-id cache is enabled and
   * {@code ops} supports that path; otherwise uses full qualified namespace SQL.
   *
   * @param mapper MyBatis mapper session
   * @param namespace parent namespace for the listed entities
   * @param ops storage operations delegate
   * @param entityType entity type stored under {@code namespace} (used to resolve the parent id)
   * @param <PO> persistent object type
   * @param <Mapper> mapper type
   * @return list from the delegate (may be empty)
   */
  public static <PO, Mapper> List<PO> listPOs(
      Mapper mapper,
      Namespace namespace,
      BasePOStorageOps<PO, Mapper> ops,
      Entity.EntityType entityType) {
    return listPOs(mapper, namespace, ops, entityType, GravitinoEnv.getInstance().cacheEnabled());
  }

  /**
   * Same as {@link #listPOs} but uses an explicit cache flag (typically for tests).
   *
   * @param cacheEnabled when true, prefer parent-id based reads when supported
   */
  public static <PO, Mapper> List<PO> listPOs(
      Mapper mapper,
      Namespace namespace,
      BasePOStorageOps<PO, Mapper> ops,
      Entity.EntityType entityType,
      boolean cacheEnabled) {
    if (cacheEnabled && ops.supportsParentIdRelationalRead()) {
      Long parentId =
          EntityIdService.getEntityId(
              NameIdentifier.parse(namespace.toString()),
              NameIdentifierUtil.parentEntityType(entityType));
      return ops.listPOs(mapper, parentId);
    }
    return ops.listPOsByNSFullName(mapper, namespace);
  }
}
