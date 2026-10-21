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
 * Runs inside policy create / update / delete transactions, with JPA-style lifecycle points
 * relative to the policy DML.
 *
 * <p>The caller already holds the open SQL session. Implementations must use that connection, and
 * must not commit, roll back, or close it. An exception rolls the policy write back together with
 * the hook's own writes.
 *
 * <p>Defaults are no-ops so an extension only overrides the points it needs.
 *
 * <p>{@code insertPolicy(..., overwritten = true)} that replaces an existing row fires the update
 * lifecycle ({@link #beforeUpdate} / {@link #afterUpdate}), not persist. A create, or an overwrite
 * that inserts because no row was found, fires persist.
 */
public interface PolicyLifecycleHook {

  /**
   * About to insert the policy row in the current transaction.
   *
   * @param policy the policy about to be stored
   */
  default void beforePersist(PolicyEntity policy) {}

  /**
   * The policy row has been inserted in the current transaction.
   *
   * @param policy the policy as stored, including id, enabled flag, and content
   */
  default void afterPersist(PolicyEntity policy) {}

  /**
   * About to update the policy row in the current transaction.
   *
   * <p>Also used when {@code insertPolicy} overwrites an existing row by name or id.
   *
   * @param policy the policy about to be stored (id is the retained row id on overwrite)
   */
  default void beforeUpdate(PolicyEntity policy) {}

  /**
   * The policy row has been updated in the current transaction.
   *
   * <p>Also used when {@code insertPolicy} overwrites an existing row by name or id.
   *
   * @param policy the policy as stored, including the retained id, enabled flag, and content
   */
  default void afterUpdate(PolicyEntity policy) {}

  /**
   * The policy row is about to be deleted in the current transaction.
   *
   * @param policyId persisted policy id
   */
  default void beforeRemove(long policyId) {}

  /**
   * The policy row has been deleted in the current transaction.
   *
   * @param policyId persisted policy id
   */
  default void afterRemove(long policyId) {}
}
