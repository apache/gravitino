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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.po.TablePO;

public class TablePOStorageOps extends BasePOStorageOps<TablePO, TableMetaMapper> {

  public TablePOStorageOps() {}

  @Override
  public void insertPO(TableMetaMapper mapper, TablePO tablePO, boolean overwrite) {
    if (overwrite) {
      mapper.insertTableMetaOnDuplicateKeyUpdate(tablePO);
    } else {
      mapper.insertTableMeta(tablePO);
    }
  }

  @Override
  public Integer updatePO(TableMetaMapper mapper, TablePO newPO, TablePO oldPO) {
    return mapper.updateTableMeta(newPO, oldPO, newPO.getSchemaId());
  }

  @Override
  public TablePO getPO(TableMetaMapper mapper, Long parentId, String name) {
    return mapper.selectTableMetaBySchemaIdAndName(parentId, name);
  }

  @Override
  protected TablePO getPOByFullName(TableMetaMapper mapper, NameIdentifier identifier) {
    Namespace namespace = identifier.namespace();
    return mapper.selectTableByFullQualifiedName(
        namespace.level(0), namespace.level(1), namespace.level(2), identifier.name());
  }

  @Override
  public List<TablePO> listPOs(TableMetaMapper mapper, Long parentId) {
    return mapper.listTablePOsBySchemaId(parentId);
  }

  @Override
  public List<TablePO> listPOs(TableMetaMapper mapper, Namespace namespace, List<String> names) {
    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(namespace.levels()), Entity.EntityType.SCHEMA);
    return mapper.batchSelectTableByIdentifier(schemaId, names);
  }

  @Override
  public List<TablePO> listPOs(TableMetaMapper mapper, List<Long> uuids) {
    return mapper.listTablePOsByTableIds(uuids);
  }

  @Override
  protected List<TablePO> listPOsByNSFullName(TableMetaMapper mapper, Namespace namespace) {
    return mapper.listTablePOsByFullQualifiedName(
        namespace.level(0), namespace.level(1), namespace.level(2));
  }

  @Override
  public List<Capability> capabilities() {
    return List.of(
        Capability.INSERT,
        Capability.UPDATE,
        Capability.GET_BY_NAME,
        Capability.GET_BY_NS_UID,
        Capability.LIST_BY_NS_UID,
        Capability.LIST_BY_NS_NAME,
        Capability.LIST_BY_NAME_FILTER,
        Capability.LIST_BY_UID_FILTER);
  }

  @Override
  protected Entity.EntityType entityType() {
    return Entity.EntityType.TABLE;
  }
}
