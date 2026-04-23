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

import java.util.regex.Pattern;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;

public class IcebergCatalogCapability implements Capability {

  private final String namespaceSeparator;

  /**
   * Creates a capability with the given external namespace separator.
   *
   * @param namespaceSeparator the external separator used in logical schema names (e.g. {@code
   *     ":"})
   */
  public IcebergCatalogCapability(String namespaceSeparator) {
    this.namespaceSeparator = namespaceSeparator;
  }

  /** Creates a capability with the default external namespace separator {@code ":"}. */
  public IcebergCatalogCapability() {
    this(":");
  }

  @Override
  public CapabilityResult columnDefaultValue() {
    // Iceberg column default value is WIP, see
    // https://github.com/apache/iceberg/pull/4525
    return CapabilityResult.unsupported("Iceberg does not support column default value.");
  }

  /**
   * Validates the schema name specification for Iceberg.
   *
   * <p>For {@link Scope#SCHEMA}, Iceberg accepts:
   *
   * <ul>
   *   <li>Regular flat schema names matching the default name pattern.
   *   <li>Logical nested schema names using the configured external separator (e.g. {@code "A:B:C"}
   *       with separator {@code ":"}), validated to have no empty segments.
   * </ul>
   *
   * For all other scopes, the default validation rules apply.
   */
  @Override
  public CapabilityResult specificationOnName(Scope scope, String name) {
    if (scope == Scope.SCHEMA && name.contains(namespaceSeparator)) {
      return validateSegments(name, namespaceSeparator);
    }
    return Capability.super.specificationOnName(scope, name);
  }

  private static CapabilityResult validateSegments(String name, String separator) {
    String[] segments = name.split(Pattern.quote(separator), -1);
    for (String segment : segments) {
      if (segment.isEmpty()) {
        return CapabilityResult.unsupported(
            String.format(
                "The SCHEMA name '%s' contains an empty segment after splitting by '%s'.",
                name, separator));
      }
    }
    return CapabilityResult.SUPPORTED;
  }
}
