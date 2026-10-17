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
package org.apache.gravitino.maintenance.pipeline;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.maintenance.pipeline.handler.CompactionMaintenanceTypeHandler;
import org.apache.gravitino.maintenance.pipeline.handler.ManifestRewriteMaintenanceTypeHandler;
import org.apache.gravitino.maintenance.pipeline.handler.OrphanCleanupMaintenanceTypeHandler;
import org.apache.gravitino.maintenance.pipeline.handler.SnapshotExpiryMaintenanceTypeHandler;
import org.apache.gravitino.policy.Policy;

/** Registry of built-in {@link MaintenanceTypeHandler}s. */
public final class MaintenanceTypeHandlers {

  private static final Map<Policy.BuiltInType, MaintenanceTypeHandler> BY_TYPE = buildRegistry();

  private MaintenanceTypeHandlers() {}

  /**
   * @param policyType built-in maintenance policy type
   * @return handler for that type
   * @throws IllegalArgumentException if the type is not a TMS maintenance kind
   */
  public static MaintenanceTypeHandler get(Policy.BuiltInType policyType) {
    MaintenanceTypeHandler handler = BY_TYPE.get(policyType);
    if (handler == null) {
      throw new IllegalArgumentException("No maintenance type handler for " + policyType);
    }
    return handler;
  }

  /**
   * Default onCommit chain inside one Spark Job: compaction → manifest-rewrite → snapshot-expiry.
   *
   * @return ordered handlers
   */
  public static List<MaintenanceTypeHandler> commitChain() {
    return Collections.unmodifiableList(
        Arrays.asList(
            get(Policy.BuiltInType.ICEBERG_COMPACTION),
            get(Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS),
            get(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION)));
  }

  /**
   * Single-type chain (cron path).
   *
   * @param policyType type to run
   * @return singleton list
   */
  public static List<MaintenanceTypeHandler> singleton(Policy.BuiltInType policyType) {
    return Collections.singletonList(get(policyType));
  }

  private static Map<Policy.BuiltInType, MaintenanceTypeHandler> buildRegistry() {
    Map<Policy.BuiltInType, MaintenanceTypeHandler> map = new EnumMap<>(Policy.BuiltInType.class);
    map.put(Policy.BuiltInType.ICEBERG_COMPACTION, new CompactionMaintenanceTypeHandler());
    map.put(
        Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS, new ManifestRewriteMaintenanceTypeHandler());
    map.put(
        Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION, new SnapshotExpiryMaintenanceTypeHandler());
    map.put(
        Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL, new OrphanCleanupMaintenanceTypeHandler());
    return Collections.unmodifiableMap(map);
  }
}
