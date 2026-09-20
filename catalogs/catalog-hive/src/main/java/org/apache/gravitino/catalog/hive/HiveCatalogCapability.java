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

import static org.apache.gravitino.hive.client.HiveClientClassLoader.HiveVersion.HIVE3;

import java.util.function.Supplier;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.hive.client.HiveClientClassLoader.HiveVersion;

/**
 * Capabilities of the Hive catalog. Column constraint support depends on the version of the
 * connected Hive Metastore, which is resolved lazily on first use.
 */
public class HiveCatalogCapability implements Capability {

  private final Supplier<HiveVersion> hiveVersion;

  /**
   * Creates a capability bound to the connected Hive Metastore version.
   *
   * @param hiveVersion Supplies the resolved Hive Metastore version.
   */
  public HiveCatalogCapability(Supplier<HiveVersion> hiveVersion) {
    this.hiveVersion = hiveVersion;
  }

  @Override
  public CapabilityResult columnNotNull() {
    // The NOT NULL constraint for column is supported since Hive 3.0, see
    // https://issues.apache.org/jira/browse/HIVE-16575
    return hive3Only("NOT NULL");
  }

  @Override
  public CapabilityResult columnDefaultValue() {
    // The DEFAULT constraint for column is supported since Hive 3.0, see
    // https://issues.apache.org/jira/browse/HIVE-18726
    return hive3Only("DEFAULT");
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

  private CapabilityResult hive3Only(String constraint) {
    HiveVersion version = hiveVersion.get();
    if (version == HIVE3) {
      return CapabilityResult.SUPPORTED;
    }
    return CapabilityResult.unsupported(
        "The "
            + constraint
            + " constraint for column is only supported since Hive 3.0, "
            + "but the connected Hive Metastore version is "
            + version
            + ".");
  }
}
