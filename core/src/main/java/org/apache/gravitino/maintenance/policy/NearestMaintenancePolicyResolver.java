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
package org.apache.gravitino.maintenance.policy;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.utils.MetadataObjectUtil;

/**
 * Selects the nearest attached maintenance policy of a given task type for a metadata object.
 *
 * <p>Direct associations on the object are preferred over ancestors (table before schema before
 * catalog). Tag-selected policies are considered only when no direct or inherited direct
 * association matches at any level.
 */
public class NearestMaintenancePolicyResolver {

  private final PolicyDispatcher policyDispatcher;

  /**
   * Creates a resolver.
   *
   * @param policyDispatcher policy dispatcher
   */
  public NearestMaintenancePolicyResolver(PolicyDispatcher policyDispatcher) {
    this.policyDispatcher = Preconditions.checkNotNull(policyDispatcher, "policyDispatcher");
  }

  /**
   * Returns the nearest enabled policy entity for the maintenance task type.
   *
   * @param metalake metalake name
   * @param metadataObject metadata object (typically a table)
   * @param taskType maintenance task type
   * @return matching policy when found
   */
  public Optional<PolicyEntity> resolveNearest(
      String metalake, MetadataObject metadataObject, TableMaintenanceTaskType taskType) {
    Preconditions.checkNotNull(metalake, "metalake cannot be null");
    MetadataObjectUtil.checkMetadataObject(metalake, metadataObject);
    Preconditions.checkNotNull(taskType, "taskType cannot be null");

    List<MetadataObject> resolutionOrder = new ArrayList<>();
    resolutionOrder.add(metadataObject);
    resolutionOrder.addAll(MetadataObjectUtil.getParentMetadataObjects(metadataObject));

    for (MetadataObject object : resolutionOrder) {
      PolicyEntity[] direct =
          policyDispatcher.listDirectPolicyInfosForMetadataObject(metalake, object);
      for (PolicyEntity policy : direct) {
        if (matchesTaskType(policy, taskType)) {
          return Optional.of(policy);
        }
      }
    }

    PolicyEntity[] effective =
        policyDispatcher.listPolicyInfosForMetadataObject(metalake, metadataObject);
    for (PolicyEntity policy : effective) {
      if (matchesTaskType(policy, taskType)) {
        return Optional.of(policy);
      }
    }
    return Optional.empty();
  }

  private static boolean matchesTaskType(PolicyEntity policy, TableMaintenanceTaskType taskType) {
    if (!policy.enabled()) {
      return false;
    }
    if (taskType.builtInType() == null) {
      return false;
    }
    return policy.policyType() == taskType.builtInType();
  }
}
