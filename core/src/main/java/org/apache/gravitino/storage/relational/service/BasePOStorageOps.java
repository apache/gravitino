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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;

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

  /**
   * When {@code true} and the entity-id cache is enabled, callers may resolve rows by parent entity
   * id plus short name via {@link #getPO} and {@link #listPOs}; see {@link POStorageReadRouting}.
   */
  public boolean supportsParentIdRelationalRead() {
    return false;
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

  public List<PO> listPOs(Mapper mapper, List<Long> entityIds) {
    throw new UnsupportedOperationException(
        "listPOs by entityIds is not supported by " + getClass().getSimpleName());
  }

  public PO getPOByFullName(Mapper mapper, NameIdentifier identifier) {
    throw new UnsupportedOperationException(
        "getPOByFullName is not supported by " + getClass().getSimpleName());
  }

  public List<PO> listPOsByNSFullName(Mapper mapper, Namespace namespace) {
    throw new UnsupportedOperationException(
        "listPOsByNSFullName is not supported by " + getClass().getSimpleName());
  }
}
