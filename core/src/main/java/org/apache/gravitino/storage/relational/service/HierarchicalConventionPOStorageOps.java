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
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;

/**
 * Wraps a {@link BasePOStorageOps} to bridge the hierarchical schema naming convention. Names that
 * appear in API form (logical separator) are translated to storage form (physical separator) before
 * delegating, and an optional rewriter can post-process POs returned from reads (typically used to
 * translate a PO field from physical back to logical for callers).
 *
 * @param <PO> persistent object type
 * @param <Mapper> MyBatis mapper type
 */
public class HierarchicalConventionPOStorageOps<PO, Mapper> extends BasePOStorageOps<PO, Mapper> {

  private final BasePOStorageOps<PO, Mapper> delegate;
  private final UnaryOperator<PO> readRewriter;

  public HierarchicalConventionPOStorageOps(BasePOStorageOps<PO, Mapper> delegate) {
    this(delegate, UnaryOperator.identity());
  }

  public HierarchicalConventionPOStorageOps(
      BasePOStorageOps<PO, Mapper> delegate, UnaryOperator<PO> readRewriter) {
    this.delegate = delegate;
    this.readRewriter = readRewriter;
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
    return applyRead(delegate.getPO(mapper, parentId, toPhysicalIfHierarchical(name)));
  }

  @Override
  public List<PO> listPOs(Mapper mapper, Long parentId) {
    return applyRead(delegate.listPOs(mapper, parentId));
  }

  @Override
  public List<PO> listPOs(Mapper mapper, Namespace namespace, List<String> names) {
    Namespace storageNs = apiNamespaceToStorage(namespace);
    List<String> storageNames =
        names.stream()
            .map(HierarchicalConventionPOStorageOps::toPhysicalIfHierarchical)
            .collect(Collectors.toList());
    return applyRead(delegate.listPOs(mapper, storageNs, storageNames));
  }

  @Override
  public List<PO> listPOs(Mapper mapper, List<Long> uuids) {
    return applyRead(delegate.listPOs(mapper, uuids));
  }

  @Override
  protected PO getPOByFullName(Mapper mapper, NameIdentifier identifier) {
    return applyRead(delegate.getPOByFullName(mapper, apiIdentifierToStorage(identifier)));
  }

  @Override
  protected List<PO> listPOsByNSFullName(Mapper mapper, Namespace namespace) {
    return applyRead(delegate.listPOsByNSFullName(mapper, apiNamespaceToStorage(namespace)));
  }

  @Override
  public List<Capability> capabilities() {
    return delegate.capabilities();
  }

  @Override
  protected Entity.EntityType entityType() {
    return delegate.entityType();
  }

  private PO applyRead(PO po) {
    return po == null ? null : readRewriter.apply(po);
  }

  private List<PO> applyRead(List<PO> pos) {
    if (pos == null || pos.isEmpty()) {
      return pos;
    }
    return pos.stream().map(this::applyRead).collect(Collectors.toList());
  }

  private static String toPhysicalIfHierarchical(String name) {
    if (StringUtils.isBlank(name)) {
      return name;
    }
    String sep = HierarchicalSchemaUtil.schemaSeparator();
    if (!name.contains(sep)) {
      return name;
    }
    return HierarchicalSchemaUtil.logicalToPhysical(name, sep);
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
