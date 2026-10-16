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
package org.apache.gravitino.policy;

import org.apache.gravitino.meta.PolicyEntity;

/**
 * Runs inside the policy transaction before it commits.
 *
 * <p>The caller already holds the open SQL session. Implementations must use that connection, and
 * must not commit, roll back, or close it. An exception rolls the policy write back together with
 * the hook's own writes.
 */
public interface PolicyPreCommitHook {

  /**
   * The policy row has been inserted or updated in the current transaction.
   *
   * @param policy the policy as stored, including id, enabled flag, and content
   */
  void afterWrite(PolicyEntity policy);

  /**
   * The policy row is about to be deleted in the current transaction.
   *
   * @param policyId persisted policy id
   */
  void beforeDelete(long policyId);
}
