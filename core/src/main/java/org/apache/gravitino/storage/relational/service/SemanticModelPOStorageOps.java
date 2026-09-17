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

import java.util.Locale;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.storage.relational.mapper.SemanticModelMetaMapper;
import org.apache.gravitino.storage.relational.po.SemanticModelPO;

/** Provides relational persistent-object operations required to create and load Semantic Models. */
public class SemanticModelPOStorageOps
    extends BasePOStorageOps<SemanticModelPO, SemanticModelMetaMapper> {

  /** Creates Semantic Model persistent-object operations. */
  public SemanticModelPOStorageOps() {}

  @Override
  public void insertPO(
      SemanticModelMetaMapper mapper, SemanticModelPO semanticModelPO, boolean overwrite) {
    if (overwrite) {
      mapper.insertSemanticModelMetaOnDuplicateKeyUpdate(semanticModelPO);
    } else {
      mapper.insertSemanticModelMeta(semanticModelPO);
    }
  }

  @Override
  public SemanticModelPO getPO(
      SemanticModelMetaMapper mapper, Long parentId, String semanticModelName) {
    return mapper.selectSemanticModelMetaBySchemaIdAndName(parentId, semanticModelName);
  }

  @Override
  public SemanticModelPO getPOByFullName(
      SemanticModelMetaMapper mapper, NameIdentifier identifier) {
    Namespace namespace = identifier.namespace();
    SemanticModelPO po =
        mapper.selectSemanticModelByFullQualifiedName(
            namespace.level(0), namespace.level(1), namespace.level(2), identifier.name());
    if (po == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.CATALOG.name().toLowerCase(Locale.ROOT),
          namespace.level(1));
    }
    if (po.getSchemaId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(Locale.ROOT),
          namespace.level(2));
    }
    if (po.getSemanticModelId() == null) {
      return null;
    }
    return po;
  }

  @Override
  public boolean supportsParentIdRelationalRead() {
    return true;
  }
}
