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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;

/**
 * Base for relational metadata services whose JDBC keys use hierarchical schema storage names.
 *
 * <p>API identifiers use the configured logical schema separator; persisted rows use the internal
 * physical separator. Subclasses implement {@link #fetchPOByStorageIdentifier} and {@link
 * #fetchPOsByStorageNamespace} after {@link #getPOForApiIdentifier} / {@link #listPOsForApiNamespace}
 * rewrite identifiers or namespaces as needed.
 *
 * @param <E> persistent object type loaded for the entity
 */
public abstract class RequireSchemaConventionService<E> {

  protected RequireSchemaConventionService() {}

  /** Loads a PO using an identifier whose naming is already in storage form. */
  protected abstract E fetchPOByStorageIdentifier(NameIdentifier storageIdentifier);

  /** Lists POs under a namespace whose naming is already in storage form where applicable. */
  protected abstract List<E> fetchPOsByStorageNamespace(Namespace storageNamespace);

  /** Converts hierarchical schema naming then loads the PO. */
  protected final E getPOForApiIdentifier(NameIdentifier apiIdentifier) {
    return fetchPOByStorageIdentifier(apiIdentifierToStorage(apiIdentifier));
  }

  /** Converts hierarchical schema naming then lists POs under the namespace. */
  protected final List<E> listPOsForApiNamespace(Namespace apiNamespace) {
    return fetchPOsByStorageNamespace(apiNamespaceToStorage(apiNamespace));
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
