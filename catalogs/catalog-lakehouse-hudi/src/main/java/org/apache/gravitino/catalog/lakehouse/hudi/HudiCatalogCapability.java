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
package org.apache.gravitino.catalog.lakehouse.hudi;

import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;

/** Describes the capabilities of the Hudi catalog backed by Hive Metastore. */
public class HudiCatalogCapability implements Capability {

  /** Creates a Hudi catalog capability for the Hive Metastore backend. */
  public HudiCatalogCapability() {}

  @Override
  public CapabilityResult caseSensitiveOnName(Scope scope) {
    if (scope == Scope.SCHEMA || scope == Scope.TABLE) {
      return CapabilityResult.unsupported(
          "The Hudi Hive backend is case insensitive for schemas and tables.");
    }
    return Capability.super.caseSensitiveOnName(scope);
  }
}
