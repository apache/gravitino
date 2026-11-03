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
package org.apache.gravitino.dto.requests;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.policy.PolicyContentDTO;
import org.apache.gravitino.policy.Policy;

/**
 * Resolves {@code policyType} wire names to content DTO classes for Jackson deserialization.
 *
 * <p>Jackson's {@code @JsonTypeInfo} matching is case-sensitive and falls back to {@link
 * PolicyContentDTO.CustomContentDTO} for any unmatched name. That makes a mis-cased built-in type
 * with built-in content fields fail as a generic malformed JSON error before request validation can
 * name the type. Callers must resolve the content class through this helper <em>before</em> reading
 * content fields.
 */
final class PolicyWireContentTypes {

  private static final Map<String, Class<? extends PolicyContentDTO>> CONTENT_BY_TYPE =
      ImmutableMap.<String, Class<? extends PolicyContentDTO>>builder()
          .put("system_iceberg_compaction", PolicyContentDTO.IcebergCompactionContentDTO.class)
          .put(
              "system_iceberg_orphan_file_removal",
              PolicyContentDTO.IcebergOrphanFileRemovalContentDTO.class)
          .put(
              "system_iceberg_rewrite_manifests",
              PolicyContentDTO.IcebergRewriteManifestsContentDTO.class)
          .put(
              "system_iceberg_snapshot_expiration",
              PolicyContentDTO.IcebergSnapshotExpirationContentDTO.class)
          .put("system_row_filter", PolicyContentDTO.RowFilterContentDTO.class)
          .put("system_column_mask", PolicyContentDTO.ColumnMaskContentDTO.class)
          .put("system_access_control", PolicyContentDTO.AccessControlContentDTO.class)
          .put("custom", PolicyContentDTO.CustomContentDTO.class)
          .build();

  private PolicyWireContentTypes() {}

  /**
   * Returns the content DTO class for an exact wire {@code policyType}.
   *
   * @param policyType the request's {@code policyType}
   * @return the matching content class, or {@link PolicyContentDTO.CustomContentDTO} for
   *     non-system, non-{@code custom} types (validated later by {@link
   *     Policy.BuiltInType#fromPolicyType(String)})
   * @throws IllegalArgumentException if {@code policyType} is blank, a mis-cased known type, or an
   *     unknown {@code system_*} type
   */
  static Class<? extends PolicyContentDTO> contentClassFor(String policyType) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(policyType), "\"policyType\" is required and cannot be empty");
    Class<? extends PolicyContentDTO> exact = CONTENT_BY_TYPE.get(policyType);
    if (exact != null) {
      return exact;
    }
    for (Map.Entry<String, Class<? extends PolicyContentDTO>> entry : CONTENT_BY_TYPE.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(policyType)) {
        throw new IllegalArgumentException(
            String.format("Policy type must be '%s', but got: %s", entry.getKey(), policyType));
      }
    }
    if (policyType.startsWith(Policy.BUILT_IN_TYPE_PREFIX)) {
      throw new IllegalArgumentException(
          String.format("Unknown built-in policy type: %s", policyType));
    }
    return PolicyContentDTO.CustomContentDTO.class;
  }
}
