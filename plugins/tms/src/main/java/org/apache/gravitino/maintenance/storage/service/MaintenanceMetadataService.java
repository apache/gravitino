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
package org.apache.gravitino.maintenance.storage.service;

import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.storage.relational.service.PolicyMetaService;

/**
 * TMS lookups against existing metadata services.
 *
 * <p>Callers in this plugin use this class. It calls {@link PolicyMetaService} and does not touch
 * its mapper.
 */
public final class MaintenanceMetadataService {

  private static final MaintenanceMetadataService INSTANCE = new MaintenanceMetadataService();

  private MaintenanceMetadataService() {}

  /**
   * Returns the shared service.
   *
   * @return the service
   */
  public static MaintenanceMetadataService getInstance() {
    return INSTANCE;
  }

  /**
   * Loads one policy by id.
   *
   * @param policyId policy id
   * @return the policy, or empty when it is missing
   */
  public Optional<PolicyEntity> getPolicyById(long policyId) {
    return Optional.ofNullable(PolicyMetaService.getInstance().getPolicyById(policyId));
  }

  /**
   * Returns the metalake name that owns the policy.
   *
   * @param policyId policy id
   * @return metalake name, or {@code null} when the policy is missing
   */
  @Nullable
  public String getMetalakeName(long policyId) {
    return getPolicyById(policyId).map(policy -> policy.namespace().level(0)).orElse(null);
  }
}
