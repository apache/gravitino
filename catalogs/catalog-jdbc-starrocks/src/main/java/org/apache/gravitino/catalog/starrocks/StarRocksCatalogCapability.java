/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.starrocks;

import org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import org.apache.gravitino.catalog.starrocks.utils.StarRocksUtils;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;

public class StarRocksCatalogCapability implements Capability {
  // StarRocks best practice mention that the name should be in lowercase, separated by underscores
  // We can use the more general DEFAULT_NAME_PATTERN for StarRocks and update as needed in the
  // future
  /** {@inheritDoc} */
  @Override
  public CapabilityResult specificationOnExistingName(Scope scope, String name) {
    if (scope == Scope.TABLE || scope == Scope.COLUMN) {
      CapabilityResult result = StarRocksUtils.checkIdentifier(name);
      if (!result.supported()) {
        return result;
      }
    }
    if (scope == Scope.TABLE) {
      return JdbcConnectorUtils.existingQuotedTableName(name);
    }
    if (scope == Scope.COLUMN) {
      return JdbcConnectorUtils.existingQuotedColumnName(name);
    }
    return specificationOnName(scope, name);
  }
}
