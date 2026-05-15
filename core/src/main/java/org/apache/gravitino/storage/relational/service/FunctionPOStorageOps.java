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
import org.apache.gravitino.storage.relational.mapper.FunctionMetaMapper;
import org.apache.gravitino.storage.relational.po.FunctionPO;

public class FunctionPOStorageOps extends BasePOStorageOps<FunctionPO, FunctionMetaMapper> {

  public FunctionPOStorageOps() {}

  @Override
  public void insertPO(FunctionMetaMapper mapper, FunctionPO functionPO, boolean overwrite) {
    if (overwrite) {
      mapper.insertFunctionMetaOnDuplicateKeyUpdate(functionPO);
    } else {
      mapper.insertFunctionMeta(functionPO);
    }
  }

  @Override
  public Integer updatePO(FunctionMetaMapper mapper, FunctionPO newPO, FunctionPO oldPO) {
    return mapper.updateFunctionMeta(newPO, oldPO);
  }

  @Override
  public FunctionPO getPO(FunctionMetaMapper mapper, Long parentId, String name) {
    return mapper.selectFunctionMetaBySchemaIdAndName(parentId, name);
  }

  @Override
  public FunctionPO getPOByFullName(FunctionMetaMapper mapper, NameIdentifier identifier) {
    Namespace namespace = identifier.namespace();
    return mapper.selectFunctionMetaByFullQualifiedName(
        namespace.level(0), namespace.level(1), namespace.level(2), identifier.name());
  }

  @Override
  public List<FunctionPO> listPOs(FunctionMetaMapper mapper, Long parentId) {
    return mapper.listFunctionPOsBySchemaId(parentId);
  }

  @Override
  public List<FunctionPO> listPOs(FunctionMetaMapper mapper, List<Long> entityIds) {
    return mapper.listFunctionPOsByFunctionIds(entityIds);
  }

  @Override
  public List<FunctionPO> listPOsByNSFullName(FunctionMetaMapper mapper, Namespace namespace) {
    List<FunctionPO> pos =
        mapper.listFunctionPOsByFullQualifiedName(
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
    if (pos.get(0).schemaId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          namespace.level(2));
    }
    // LEFT JOIN on function_meta: filter out the placeholder row for a schema without functions.
    return pos.stream().filter(po -> po.functionId() != null).collect(Collectors.toList());
  }

  @Override
  public boolean supportsParentIdRelationalRead() {
    return true;
  }

  @Override
  protected Entity.EntityType entityType() {
    return Entity.EntityType.FUNCTION;
  }
}
