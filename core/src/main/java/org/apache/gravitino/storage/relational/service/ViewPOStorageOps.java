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
import org.apache.gravitino.storage.relational.mapper.ViewMetaMapper;
import org.apache.gravitino.storage.relational.po.ViewPO;

public class ViewPOStorageOps extends BasePOStorageOps<ViewPO, ViewMetaMapper> {

  public ViewPOStorageOps() {}

  @Override
  public void insertPO(ViewMetaMapper mapper, ViewPO viewPO, boolean overwrite) {
    if (overwrite) {
      mapper.insertViewMetaOnDuplicateKeyUpdate(viewPO);
    } else {
      mapper.insertViewMeta(viewPO);
    }
  }

  @Override
  public Integer updatePO(ViewMetaMapper mapper, ViewPO newPO, ViewPO oldPO) {
    return mapper.updateViewMeta(newPO, oldPO);
  }

  @Override
  public ViewPO getPO(ViewMetaMapper mapper, Long parentId, String name) {
    return mapper.selectViewMetaBySchemaIdAndName(parentId, name);
  }

  @Override
  protected ViewPO getPOByFullName(ViewMetaMapper mapper, NameIdentifier identifier) {
    Namespace namespace = identifier.namespace();
    return mapper.selectViewByFullQualifiedName(
        namespace.level(0), namespace.level(1), namespace.level(2), identifier.name());
  }

  @Override
  public List<ViewPO> listPOs(ViewMetaMapper mapper, Long parentId) {
    return mapper.listViewPOsBySchemaId(parentId);
  }

  @Override
  public List<ViewPO> listPOs(ViewMetaMapper mapper, List<Long> uuids) {
    return mapper.listViewPOsByViewIds(uuids);
  }

  @Override
  protected List<ViewPO> listPOsByNSFullName(ViewMetaMapper mapper, Namespace namespace) {
    return mapper.listViewPOsByFullQualifiedName(
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
        Capability.LIST_BY_UID_FILTER);
  }

  @Override
  protected Entity.EntityType entityType() {
    return Entity.EntityType.VIEW;
  }
}
