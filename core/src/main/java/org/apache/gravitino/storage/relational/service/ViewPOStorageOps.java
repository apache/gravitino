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
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
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
  public ViewPO getPOByFullName(ViewMetaMapper mapper, NameIdentifier identifier) {
    Namespace namespace = identifier.namespace();
    ViewPO po =
        mapper.selectViewByFullQualifiedName(
            namespace.level(0), namespace.level(1), namespace.level(2), identifier.name());
    // INNER JOIN on metalake/catalog: a null PO means the metalake or catalog does not exist.
    if (po == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.CATALOG.name().toLowerCase(),
          namespace.level(1));
    }
    // LEFT JOIN on schema_meta: a row with non-null catalogId but null schemaId means
    // the catalog exists but the schema does not.
    if (po.getSchemaId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          namespace.level(2));
    }
    // LEFT JOIN on view_meta: a row with non-null schemaId but null viewId means
    // the schema exists but the view does not.
    if (po.getViewId() == null) {
      return null;
    }
    return po;
  }

  @Override
  public List<ViewPO> listPOs(ViewMetaMapper mapper, Long parentId) {
    return mapper.listViewPOsBySchemaId(parentId);
  }

  @Override
  public List<ViewPO> listPOs(ViewMetaMapper mapper, List<Long> entityIds) {
    return mapper.listViewPOsByViewIds(entityIds);
  }

  @Override
  public List<ViewPO> listPOsByNSFullName(ViewMetaMapper mapper, Namespace namespace) {
    List<ViewPO> pos =
        mapper.listViewPOsByFullQualifiedName(
            namespace.level(0), namespace.level(1), namespace.level(2));
    // INNER JOIN on metalake/catalog: an empty result means the metalake or catalog does not exist.
    if (pos.isEmpty()) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.CATALOG.name().toLowerCase(),
          namespace.level(1));
    }
    // LEFT JOIN on schema_meta: rows with non-null catalogId but null schemaId mean the
    // catalog exists but the schema does not.
    if (pos.get(0).getSchemaId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          namespace.level(2));
    }
    // LEFT JOIN on view_meta: filter out the placeholder row for a schema without views.
    return pos.stream().filter(po -> po.getViewId() != null).collect(Collectors.toList());
  }

  @Override
  public boolean supportsParentIdRelationalRead() {
    return true;
  }
}
