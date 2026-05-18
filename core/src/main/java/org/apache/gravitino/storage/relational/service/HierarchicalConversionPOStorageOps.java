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
 * Wraps a {@link BasePOStorageOps} to bridge the hierarchical schema naming convention. Identifiers
 * and namespace segments in logical form (logical separator) are translated to physical form
 * (physical separator) before delegating. Two optional PO rewriters allow callers to translate a PO
 * field across the boundary: {@code physicalToLogicalRewriter} is applied to POs returned from read
 * methods (typically physical→logical), and {@code logicalToPhysicalRewriter} is applied to POs
 * passed into write methods (typically logical→physical) so the SQL still receives storage-form
 * values.
 *
 * @param <PO> persistent object type
 * @param <Mapper> MyBatis mapper type
 */
public class HierarchicalConversionPOStorageOps<PO, Mapper> extends BasePOStorageOps<PO, Mapper> {

  private final BasePOStorageOps<PO, Mapper> delegate;
  private final UnaryOperator<PO> physicalToLogicalRewriter;
  private final UnaryOperator<PO> logicalToPhysicalRewriter;

  public HierarchicalConversionPOStorageOps(BasePOStorageOps<PO, Mapper> delegate) {
    this(delegate, UnaryOperator.identity(), UnaryOperator.identity());
  }

  public HierarchicalConversionPOStorageOps(
      BasePOStorageOps<PO, Mapper> delegate,
      UnaryOperator<PO> physicalToLogicalRewriter,
      UnaryOperator<PO> logicalToPhysicalRewriter) {
    this.delegate = delegate;
    this.physicalToLogicalRewriter = physicalToLogicalRewriter;
    this.logicalToPhysicalRewriter = logicalToPhysicalRewriter;
  }

  @Override
  public void insertPO(Mapper mapper, PO po, boolean overwrite) {
    delegate.insertPO(mapper, logicalToPhysicalRewriter.apply(po), overwrite);
  }

  @Override
  public void batchInsertPOs(Mapper mapper, List<PO> pos, boolean overwrite) {
    delegate.batchInsertPOs(mapper, applyWrite(pos), overwrite);
  }

  @Override
  public Integer updatePO(Mapper mapper, PO newPO, PO oldPO) {
    return delegate.updatePO(
        mapper, logicalToPhysicalRewriter.apply(newPO), logicalToPhysicalRewriter.apply(oldPO));
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
  public List<PO> listPOs(Mapper mapper, Namespace logicalNamespace, List<String> names) {
    Namespace physicalNamespace = logicalToPhysicalNamespace(logicalNamespace);
    List<String> physicalNames =
        names.stream()
            .map(HierarchicalConversionPOStorageOps::toPhysicalIfHierarchical)
            .collect(Collectors.toList());
    return applyRead(delegate.listPOs(mapper, physicalNamespace, physicalNames));
  }

  @Override
  public List<PO> listPOs(Mapper mapper, List<Long> entityIds) {
    return applyRead(delegate.listPOs(mapper, entityIds));
  }

  @Override
  public PO getPOByFullName(Mapper mapper, NameIdentifier logical) {
    NameIdentifier physical = logicalToPhysicalIdentifier(logical);
    return applyRead(delegate.getPOByFullName(mapper, physical));
  }

  @Override
  public List<PO> listPOsByNSFullName(Mapper mapper, Namespace logical) {
    Namespace physical = logicalToPhysicalNamespace(logical);
    return applyRead(delegate.listPOsByNSFullName(mapper, physical));
  }

  @Override
  public boolean supportsParentIdRelationalRead() {
    return delegate.supportsParentIdRelationalRead();
  }

  @Override
  protected Entity.EntityType entityType() {
    return delegate.entityType();
  }

  private PO applyRead(PO po) {
    return po == null ? null : physicalToLogicalRewriter.apply(po);
  }

  private List<PO> applyRead(List<PO> pos) {
    if (pos == null || pos.isEmpty()) {
      return pos;
    }
    return pos.stream().map(this::applyRead).collect(Collectors.toList());
  }

  private List<PO> applyWrite(List<PO> pos) {
    if (pos == null || pos.isEmpty()) {
      return pos;
    }
    return pos.stream().map(logicalToPhysicalRewriter).collect(Collectors.toList());
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

  private static NameIdentifier logicalToPhysicalIdentifier(NameIdentifier logical) {
    String[] levels = logical.namespace().levels();
    if (levels.length == 2) {
      String rawName = logical.name();
      String physicalName =
          StringUtils.isNotBlank(rawName)
              ? HierarchicalSchemaUtil.logicalToPhysical(
                  rawName, HierarchicalSchemaUtil.schemaSeparator())
              : rawName;
      if (physicalName.equals(logical.name())) {
        return logical;
      }
      return NameIdentifier.of(logical.namespace(), physicalName);
    }
    if (levels.length == 3) {
      String rawSeg = levels[2];
      String physicalSchema =
          StringUtils.isNotBlank(rawSeg)
              ? HierarchicalSchemaUtil.logicalToPhysical(
                  rawSeg, HierarchicalSchemaUtil.schemaSeparator())
              : rawSeg;
      if (physicalSchema.equals(levels[2])) {
        return logical;
      }
      return NameIdentifier.of(Namespace.of(levels[0], levels[1], physicalSchema), logical.name());
    }
    return logical;
  }

  private static Namespace logicalToPhysicalNamespace(Namespace logical) {
    String[] levels = logical.levels();
    if (levels.length != 3) {
      return logical;
    }
    String rawSeg = levels[2];
    String physicalSchema =
        StringUtils.isNotBlank(rawSeg)
            ? HierarchicalSchemaUtil.logicalToPhysical(
                rawSeg, HierarchicalSchemaUtil.schemaSeparator())
            : rawSeg;
    if (physicalSchema.equals(levels[2])) {
      return logical;
    }
    return Namespace.of(levels[0], levels[1], physicalSchema);
  }
}
