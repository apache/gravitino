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
  private final String schemaNameSeparator;

  public IcebergCatalogCapability() {
    this(".");
  }

  public IcebergCatalogCapability(String schemaNameSeparator) {
    this.schemaNameSeparator = schemaNameSeparator == null ? "." : schemaNameSeparator;
  }

  @Override
  public CapabilityResult columnDefaultValue() {
    // Iceberg column default value is WIP, see
    // https://github.com/apache/iceberg/pull/4525
    return CapabilityResult.unsupported("Iceberg does not support column default value.");
  }

  @Override
  public CapabilityResult specificationOnName(Scope scope, String name) {
    if (scope != Scope.SCHEMA || !name.contains(schemaNameSeparator)) {
      return Capability.super.specificationOnName(scope, name);
    }

    String[] segments = name.split(Pattern.quote(schemaNameSeparator), -1);
    if (segments.length == 0) {
      return CapabilityResult.unsupported(
          String.format("The %s name '%s' is illegal.", scope, name));
    }

    for (String segment : segments) {
      if (segment.isEmpty()) {
        return CapabilityResult.unsupported(
            String.format("The %s name '%s' is illegal.", scope, name));
      }
      CapabilityResult result = Capability.super.specificationOnName(scope, segment);
      if (!result.supported()) {
        return result;
      }
    }

    return CapabilityResult.SUPPORTED;
  }
}
