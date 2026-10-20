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
package org.apache.gravitino.maintenance.scheduler;

import java.util.Optional;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.IcebergOrphanFileRemovalContent;
import org.apache.gravitino.policy.IcebergRewriteManifestsContent;
import org.apache.gravitino.policy.IcebergSnapshotExpirationContent;
import org.apache.gravitino.policy.Policy;

/** Maps maintenance {@link Policy.BuiltInType} to built-in Spark job template names. */
public final class MaintenanceJobTemplates {

  private MaintenanceJobTemplates() {}

  /**
   * @param builtInType maintenance policy type
   * @return built-in job template name when known
   */
  public static Optional<String> templateName(Policy.BuiltInType builtInType) {
    if (builtInType == null) {
      return Optional.empty();
    }
    switch (builtInType) {
      case ICEBERG_COMPACTION:
        return Optional.of(IcebergDataCompactionContent.JOB_TEMPLATE_NAME_VALUE);
      case ICEBERG_REWRITE_MANIFESTS:
        return Optional.of(IcebergRewriteManifestsContent.JOB_TEMPLATE_NAME_VALUE);
      case ICEBERG_SNAPSHOT_EXPIRATION:
        return Optional.of(IcebergSnapshotExpirationContent.JOB_TEMPLATE_NAME_VALUE);
      case ICEBERG_ORPHAN_FILE_REMOVAL:
        return Optional.of(IcebergOrphanFileRemovalContent.JOB_TEMPLATE_NAME_VALUE);
      default:
        return Optional.empty();
    }
  }
}
