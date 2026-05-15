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
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.po.SchemaPO;

public class SchemaPOStorageOps extends BasePOStorageOps<SchemaPO, SchemaMetaMapper> {

  public SchemaPOStorageOps() {}

  @Override
  public void batchInsertPOs(SchemaMetaMapper mapper, List<SchemaPO> schemaPOs, boolean overwrite) {
    if (overwrite) {
      mapper.batchInsertSchemaMetaOnDuplicateKeyUpdate(schemaPOs);
    } else {
      mapper.batchInsertSchemaMeta(schemaPOs);
    }
  }

  @Override
  public Integer updatePO(SchemaMetaMapper mapper, SchemaPO oldPO, SchemaPO newPO) {
    return mapper.updateSchemaMeta(oldPO, newPO);
  }

  @Override
  public SchemaPO getPO(SchemaMetaMapper mapper, Long parentId, String name) {
    return mapper.selectSchemaMetaByCatalogIdAndName(parentId, name);
  }

  @Override
  public SchemaPO getPOByFullName(SchemaMetaMapper mapper, NameIdentifier identifier) {
    Namespace namespace = identifier.namespace();
    SchemaPO po =
        mapper.selectSchemaByFullQualifiedName(
            namespace.level(0), namespace.level(1), identifier.name());
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
      return null;
    }
    return po;
  }

  @Override
  public List<SchemaPO> listPOs(SchemaMetaMapper schemaMetaMapper, Long parentId) {
    return schemaMetaMapper.listSchemaPOsByCatalogId(parentId);
  }

  @Override
  public List<SchemaPO> listPOs(
      SchemaMetaMapper schemaMetaMapper, Namespace namespace, List<String> names) {
    return schemaMetaMapper.batchSelectSchemaByIdentifier(
        namespace.level(0), namespace.level(1), names);
  }

  @Override
  public List<SchemaPO> listPOs(SchemaMetaMapper schemaMetaMapper, List<Long> entityIds) {
    return schemaMetaMapper.listSchemaPOsBySchemaIds(entityIds);
  }

  @Override
  public List<SchemaPO> listPOsByNSFullName(
      SchemaMetaMapper schemaMetaMapper, Namespace namespace) {
    List<SchemaPO> pos =
        schemaMetaMapper.listSchemaPOsByFullQualifiedName(namespace.level(0), namespace.level(1));
    // An empty result means the parent metalake or catalog does not exist.
    if (pos.isEmpty()) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.CATALOG.name().toLowerCase(),
          namespace.level(1));
    }
    // Same LEFT JOIN behavior as getPOByFullName: filter out the placeholder row that
    // represents an existing catalog without any matching schema.
    return pos.stream().filter(po -> po.getSchemaId() != null).collect(Collectors.toList());
  }

  @Override
  public boolean supportsParentIdRelationalRead() {
    return true;
  }

  @Override
  protected Entity.EntityType entityType() {
    return Entity.EntityType.SCHEMA;
  }
}
