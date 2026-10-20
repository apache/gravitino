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

import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;

/** Result of nearest maintenance-policy resolution for one table and built-in type. */
final class NearestPolicyDecision {

  enum Kind {
    /** Exactly one matching policy on the nearest non-empty level. */
    WINNER,
    /** No matching policy on the inheritance chain. */
    NONE,
    /** Two or more same-type policies on the nearest non-empty level. */
    CONFLICT
  }

  private final Kind kind;
  @Nullable private final Long winnerPolicyId;
  @Nullable private final MetadataObject conflictLevel;
  private final List<Long> conflictingPolicyIds;

  private NearestPolicyDecision(
      Kind kind,
      @Nullable Long winnerPolicyId,
      @Nullable MetadataObject conflictLevel,
      List<Long> conflictingPolicyIds) {
    this.kind = kind;
    this.winnerPolicyId = winnerPolicyId;
    this.conflictLevel = conflictLevel;
    this.conflictingPolicyIds = conflictingPolicyIds;
  }

  static NearestPolicyDecision winner(long policyId) {
    return new NearestPolicyDecision(Kind.WINNER, policyId, null, List.of());
  }

  static NearestPolicyDecision none() {
    return new NearestPolicyDecision(Kind.NONE, null, null, List.of());
  }

  static NearestPolicyDecision conflict(MetadataObject level, List<Long> policyIds) {
    return new NearestPolicyDecision(Kind.CONFLICT, null, level, List.copyOf(policyIds));
  }

  Kind kind() {
    return kind;
  }

  Optional<Long> winnerPolicyId() {
    return Optional.ofNullable(winnerPolicyId);
  }

  @Nullable
  MetadataObject conflictLevel() {
    return conflictLevel;
  }

  List<Long> conflictingPolicyIds() {
    return conflictingPolicyIds;
  }
}
