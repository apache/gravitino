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
package org.apache.gravitino.maintenance.scheduler.commit;

import com.google.common.base.Preconditions;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;

/** One onCommit maintenance type in the commit-job chain (§5.2.3). */
public final class CommitChainMember {

  private final Policy.BuiltInType policyType;
  private final PolicyEntity policy;
  private final String maintenanceType;

  /**
   * @param policyType built-in maintenance type
   * @param policy nearest enabled policy with {@code commitTrigger=true}
   * @param maintenanceType Spark wire name ({@code rewrite_data_files}, …)
   */
  public CommitChainMember(
      Policy.BuiltInType policyType, PolicyEntity policy, String maintenanceType) {
    this.policyType = Preconditions.checkNotNull(policyType);
    this.policy = Preconditions.checkNotNull(policy);
    this.maintenanceType = Preconditions.checkNotNull(maintenanceType);
  }

  /**
   * @return built-in policy type
   */
  public Policy.BuiltInType policyType() {
    return policyType;
  }

  /**
   * @return policy entity
   */
  public PolicyEntity policy() {
    return policy;
  }

  /**
   * @return Spark {@code maintenance_type} wire value
   */
  public String maintenanceType() {
    return maintenanceType;
  }
}
