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
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;

/**
 * Wraps a {@link BasePOStorageOps} so full-name lookups translate the schema segment from the
 * external logical separator used by the API into the internal physical separator used in storage.
 * Cache-keyed lookups already use API-form names, so they pass through unchanged.
 *
 * @param <PO> persistent object type
 * @param <Mapper> MyBatis mapper type
 */
public class HierarchicalSchemaPOStorageOps<PO, Mapper> extends BasePOStorageOps<PO, Mapper> {

  private final BasePOStorageOps<PO, Mapper> delegate;

  public HierarchicalSchemaPOStorageOps(BasePOStorageOps<PO, Mapper> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void insertPO(Mapper mapper, PO po, boolean overwrite) {
    delegate.insertPO(mapper, po, overwrite);
  }

  @Override
  public void batchInsertPOs(Mapper mapper, List<PO> pos, boolean overwrite) {
    delegate.batchInsertPOs(mapper, pos, overwrite);
  }

  @Override
  public Integer updatePO(Mapper mapper, PO newPO, PO oldPO) {
    return delegate.updatePO(mapper, newPO, oldPO);
  }

  @Override
  public PO getPO(Mapper mapper, Long parentId, String name) {
    return delegate.getPO(mapper, parentId, name);
  }

  @Override
  public List<PO> listPOs(Mapper mapper, Long parentId) {
    return delegate.listPOs(mapper, parentId);
  }

  @Override
  public List<PO> listPOs(Mapper mapper, Namespace namespace, List<String> names) {
    return delegate.listPOs(mapper, namespace, names);
  }

  @Override
  public List<PO> listPOs(Mapper mapper, List<Long> uuids) {
    return delegate.listPOs(mapper, uuids);
  }

  @Override
  protected PO getPOByFullName(Mapper mapper, NameIdentifier identifier) {
    return delegate.getPOByFullName(mapper, apiIdentifierToStorage(identifier));
  }

  @Override
  protected List<PO> listPOsByNSFullName(Mapper mapper, Namespace namespace) {
    return delegate.listPOsByNSFullName(mapper, apiNamespaceToStorage(namespace));
  }

  @Override
  public List<Capability> capabilities() {
    return delegate.capabilities();
  }

  @Override
  protected Entity.EntityType entityType() {
    return delegate.entityType();
  }

  private static NameIdentifier apiIdentifierToStorage(NameIdentifier apiIdentifier) {
    String[] levels = apiIdentifier.namespace().levels();
    if (levels.length == 2) {
      String rawName = apiIdentifier.name();
      String storageName =
          StringUtils.isNotBlank(rawName)
              ? HierarchicalSchemaUtil.logicalToPhysical(
                  rawName, HierarchicalSchemaUtil.schemaSeparator())
              : rawName;
      if (storageName.equals(apiIdentifier.name())) {
        return apiIdentifier;
      }
      return NameIdentifier.of(apiIdentifier.namespace(), storageName);
    }
    if (levels.length == 3) {
      String rawSeg = levels[2];
      String storageSchema =
          StringUtils.isNotBlank(rawSeg)
              ? HierarchicalSchemaUtil.logicalToPhysical(
                  rawSeg, HierarchicalSchemaUtil.schemaSeparator())
              : rawSeg;
      if (storageSchema.equals(levels[2])) {
        return apiIdentifier;
      }
      return NameIdentifier.of(
          Namespace.of(levels[0], levels[1], storageSchema), apiIdentifier.name());
    }
    return apiIdentifier;
  }

  private static Namespace apiNamespaceToStorage(Namespace apiNamespace) {
    String[] levels = apiNamespace.levels();
    if (levels.length != 3) {
      return apiNamespace;
    }
    String rawSeg = levels[2];
    String storageSchema =
        StringUtils.isNotBlank(rawSeg)
            ? HierarchicalSchemaUtil.logicalToPhysical(
                rawSeg, HierarchicalSchemaUtil.schemaSeparator())
            : rawSeg;
    if (storageSchema.equals(levels[2])) {
      return apiNamespace;
    }
    return Namespace.of(levels[0], levels[1], storageSchema);
  }
}
