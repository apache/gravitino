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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;

public class IcebergCatalogCapability implements Capability {

  private final String schemaSeparator;

  private final boolean hiveBackend;

  /**
   * Creates a capability with the given external schema separator.
   *
   * @param schemaSeparator the external separator used in logical schema names (e.g. {@code ":"})
   */
  public IcebergCatalogCapability(String schemaSeparator) {
    this(schemaSeparator, false);
  }

  /**
   * Creates a capability with backend-specific identifier handling.
   *
   * @param schemaSeparator The external separator used in logical schema names.
   * @param hiveBackend Whether the catalog uses the case-insensitive Hive metastore.
   */
  public IcebergCatalogCapability(String schemaSeparator, boolean hiveBackend) {
    this.schemaSeparator = schemaSeparator;
    this.hiveBackend = hiveBackend;
  }

  @Override
  public CapabilityResult caseSensitiveOnName(Scope scope) {
    if (hiveBackend && (scope == Scope.SCHEMA || scope == Scope.TABLE)) {
      // Hive normalizes database and table names, but Iceberg column names retain their case.
      return CapabilityResult.unsupported(
          "The Hive backend is case insensitive for schema and table names.");
    }
    return Capability.super.caseSensitiveOnName(scope);
  }

  @Override
  public CapabilityResult columnDefaultValue() {
    // Iceberg column default value is WIP, see
    // https://github.com/apache/iceberg/pull/4525
    return CapabilityResult.unsupported("Iceberg does not support column default value.");
  }

  @Override
  public CapabilityResult supportsHierarchicalSchema() {
    // Iceberg namespaces can be multi-level, so Iceberg supports hierarchical (nested) schemas.
    return CapabilityResult.SUPPORTED;
  }

  /**
   * Validates the schema name specification for Iceberg.
   *
   * <p>For {@link Scope#SCHEMA}, Iceberg accepts:
   *
   * <ul>
   *   <li>Regular flat schema names matching the default name pattern.
   *   <li>Logical hierarchical schema names using the configured external separator (e.g. {@code
   *       "A:B:C"} with separator {@code ":"}). Each segment must be non-empty and individually
   *       satisfy the default SCHEMA name rules (reserved words and name pattern).
   * </ul>
   *
   * <p>For all other scopes, the default validation rules apply.
   */
  @Override
  public CapabilityResult specificationOnName(Scope scope, String name) {
    if (scope == Scope.SCHEMA && name.contains(schemaSeparator)) {
      return validateSegments(name, schemaSeparator);
    }
    return Capability.super.specificationOnName(scope, name);
  }

  private CapabilityResult validateSegments(String name, String separator) {
    String[] segments = HierarchicalSchemaUtil.splitSchemaName(name, separator);
    for (String segment : segments) {
      if (segment.isEmpty()) {
        return CapabilityResult.unsupported(
            String.format(
                "The SCHEMA name '%s' contains an empty segment after splitting by '%s'.",
                name, separator));
      }
      CapabilityResult segmentResult = Capability.super.specificationOnName(Scope.SCHEMA, segment);
      if (!segmentResult.supported()) {
        return CapabilityResult.unsupported(
            String.format(
                "The SCHEMA name '%s' contains an illegal segment '%s': %s",
                name, segment, segmentResult.unsupportedMessage()));
      }
    }
    return CapabilityResult.SUPPORTED;
  }
}
