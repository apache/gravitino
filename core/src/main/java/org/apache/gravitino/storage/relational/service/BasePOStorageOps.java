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

public abstract class BasePOStorageOps<PO, Mapper> {
  public void insertPO(Mapper mapper, PO po, boolean overwrite) {
    throw new UnsupportedOperationException(
        "insertPO is not supported by " + getClass().getSimpleName());
  }

  public void batchInsertPOs(Mapper mapper, List<PO> pos, boolean overwrite) {
    throw new UnsupportedOperationException(
        "batchInsertPOs is not supported by " + getClass().getSimpleName());
  }

  public Integer updatePO(Mapper mapper, PO newPO, PO oldPO) {
    throw new UnsupportedOperationException(
        "updatePO is not supported by " + getClass().getSimpleName());
  }

  public final PO getPO(Mapper mapper, NameIdentifier identifier) {
    if (!capabilities().contains(Capability.GET_BY_NS_UID)
        && !capabilities().contains(Capability.GET_BY_NAME)) {
      throw new UnsupportedOperationException(
          "getPO requires GET_BY_NS_UID or GET_BY_NAME for "
              + entityType()
              + ", but capabilities are "
              + capabilities());
    }

    if (GravitinoEnv.getInstance().cacheEnabled()
        && capabilities().contains(Capability.GET_BY_NS_UID)) {
      Long parentId =
          EntityIdService.getEntityId(
              NameIdentifier.parse(identifier.namespace().toString()),
              NameIdentifierUtil.parentEntityType(entityType()));
      return getPO(mapper, parentId, identifier.name());
    }

    return getPOByFullName(mapper, identifier);
  }

  public final List<PO> listPOs(Mapper mapper, Namespace namespace) {
    if (!capabilities().contains(Capability.LIST_BY_NS_UID)
        && !capabilities().contains(Capability.LIST_BY_NS_NAME)) {
      throw new UnsupportedOperationException(
          "listPOs requires LIST_BY_NS_UID or LIST_BY_NS_NAME for "
              + entityType()
              + ", but capabilities are "
              + capabilities());
    }

    if (GravitinoEnv.getInstance().cacheEnabled()
        && capabilities().contains(Capability.LIST_BY_NS_UID)) {
      Long parentId =
          EntityIdService.getEntityId(
              NameIdentifier.parse(namespace.toString()),
              NameIdentifierUtil.parentEntityType(entityType()));
      return listPOs(mapper, parentId);
    }

    return listPOsByNSFullName(mapper, namespace);
  }

  public PO getPO(Mapper mapper, Long parentId, String name) {
    throw new UnsupportedOperationException(
        "getPO by parent id is not supported by " + getClass().getSimpleName());
  }

  public List<PO> listPOs(Mapper mapper, Long parentId) {
    throw new UnsupportedOperationException(
        "listPOs by parent id is not supported by " + getClass().getSimpleName());
  }

  public List<PO> listPOs(Mapper mapper, Namespace namespace, List<String> names) {
    throw new UnsupportedOperationException(
        "listPOs by namespace and names is not supported by " + getClass().getSimpleName());
  }

  public List<PO> listPOs(Mapper mapper, List<Long> uuids) {
    throw new UnsupportedOperationException(
        "listPOs by uuids is not supported by " + getClass().getSimpleName());
  }

  protected PO getPOByFullName(Mapper mapper, NameIdentifier identifier) {
    throw new UnsupportedOperationException(
        "getPOByFullName is not supported by " + getClass().getSimpleName());
  }

  protected List<PO> listPOsByNSFullName(Mapper mapper, Namespace namespace) {
    throw new UnsupportedOperationException(
        "listPOsByNSFullName is not supported by " + getClass().getSimpleName());
  }

  public abstract List<Capability> capabilities();

  protected abstract Entity.EntityType entityType();

  public enum Capability {
    INSERT,
    BATCH_INSERT,
    UPDATE,
    GET_BY_NAME,
    GET_BY_NS_UID,
    LIST_BY_NS_UID,
    LIST_BY_NS_NAME,
    LIST_BY_NAME_FILTER,
    LIST_BY_UID_FILTER
  }
}
