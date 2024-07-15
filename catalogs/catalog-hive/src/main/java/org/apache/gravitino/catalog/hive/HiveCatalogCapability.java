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
package org.apache.gravitino.catalog.hive;

import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;

public class HiveCatalogCapability implements Capability {
  @Override
  public CapabilityResult columnNotNull() {
    // The NOT NULL constraint for column is supported since Hive 3.0, see
    // https://issues.apache.org/jira/browse/HIVE-16575
    return CapabilityResult.unsupported(
        "The NOT NULL constraint for column is only supported since Hive 3.0, "
            + "but the current Gravitino Hive catalog only supports Hive 2.x.");
  }

  @Override
  public CapabilityResult columnDefaultValue() {
    // The DEFAULT constraint for column is supported since Hive 3.0, see
    // https://issues.apache.org/jira/browse/HIVE-18726
    return CapabilityResult.unsupported(
        "The DEFAULT constraint for column is only supported since Hive 3.0, "
            + "but the current Gravitino Hive catalog only supports Hive 2.x.");
  }

  @Override
  public CapabilityResult caseSensitiveOnName(Scope scope) {
    switch (scope) {
      case SCHEMA:
      case TABLE:
      case COLUMN:
        // Hive is case insensitive, see
        // https://cwiki.apache.org/confluence/display/Hive/User+FAQ#UserFAQ-AreHiveSQLidentifiers(e.g.tablenames,columnnames,etc)casesensitive?
        return CapabilityResult.unsupported("Hive is case insensitive.");
      default:
        return CapabilityResult.SUPPORTED;
    }
  }
}
