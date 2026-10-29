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
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.storage.relational.mapper.OntologyMetaMapper;
import org.apache.gravitino.storage.relational.po.OntologyPO;

/** Provides relational persistent-object operations for ontologies. */
public class OntologyPOStorageOps extends BasePOStorageOps<OntologyPO, OntologyMetaMapper> {

  /** Creates ontology persistent-object operations. */
  public OntologyPOStorageOps() {}

  /**
   * {@inheritDoc}
   *
   * <p>Overwrite is handled by {@link OntologyMetaService} through a locked, version-checked
   * update. A database-specific upsert would bypass the persisted identity and version resolution.
   */
  @Override
  public void insertPO(OntologyMetaMapper mapper, OntologyPO ontologyPO, boolean overwrite) {
    Preconditions.checkArgument(
        !overwrite, "Ontology overwrite is handled by OntologyMetaService, not by an upsert");
    mapper.insertOntologyMeta(ontologyPO);
  }

  @Override
  public Integer updatePO(OntologyMetaMapper mapper, OntologyPO newPO, OntologyPO oldPO) {
    return mapper.updateOntologyMeta(newPO, oldPO);
  }

  @Override
  public OntologyPO getPO(OntologyMetaMapper mapper, Long parentId, String ontologyName) {
    return mapper.selectOntologyMetaByMetalakeIdAndName(parentId, ontologyName);
  }

  @Override
  public OntologyPO getPOByFullName(OntologyMetaMapper mapper, NameIdentifier identifier) {
    Namespace namespace = identifier.namespace();
    OntologyPO po = mapper.selectOntologyByFullQualifiedName(namespace.level(0), identifier.name());
    if (po == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.METALAKE.name().toLowerCase(Locale.ROOT),
          namespace.level(0));
    }
    if (po.getOntologyId() == null) {
      return null;
    }
    return po;
  }

  @Override
  public List<OntologyPO> listPOs(OntologyMetaMapper mapper, Long parentId) {
    return mapper.listOntologyPOsByMetalakeId(parentId);
  }

  @Override
  public List<OntologyPO> listPOsByNSFullName(OntologyMetaMapper mapper, Namespace namespace) {
    List<OntologyPO> pos = mapper.listOntologyPOsByFullQualifiedName(namespace.level(0));
    if (pos.isEmpty()) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.METALAKE.name().toLowerCase(Locale.ROOT),
          namespace.level(0));
    }
    return pos.stream().filter(po -> po.getOntologyId() != null).collect(Collectors.toList());
  }

  /**
   * {@inheritDoc}
   *
   * <p>An ontology is addressed by its metalake and name only, so it is always read through the
   * metalake-name join rather than through a cached parent identifier. The metalake is the parent,
   * and resolving it by name is cheap; the parent-id path would need the ontology type registered
   * with the entity-id resolver, which this storage shape does not require.
   */
  @Override
  public boolean supportsParentIdRelationalRead() {
    return false;
  }
}
