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

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.PolicyEntity;

/** Carries the policy content checked by the current synchronous authorization request. */
public final class PolicyEditAuthorization implements AutoCloseable {
  private static final ThreadLocal<PolicyEditAuthorization> CURRENT = new ThreadLocal<>();

  @Nullable private final PolicyEditAuthorization previous;
  @Nullable private PolicyEntity checked;
  private boolean requireUnbound;
  private boolean requireEnabled;

  private PolicyEditAuthorization(@Nullable PolicyEditAuthorization previous) {
    this.previous = previous;
  }

  /**
   * Opens a request scope, restoring any enclosing scope when closed.
   *
   * @return the new scope
   */
  public static PolicyEditAuthorization open() {
    PolicyEditAuthorization scope = new PolicyEditAuthorization(CURRENT.get());
    CURRENT.set(scope);
    return scope;
  }

  /**
   * Records the policy whose content was used for the current edit authorization.
   *
   * @param policy the policy read by authorization
   */
  public static void record(PolicyEntity policy) {
    PolicyEditAuthorization scope = CURRENT.get();
    if (scope != null) {
      scope.checked = policy;
    }
  }

  /** Records that enable was authorized as a no-op on an already enabled policy. */
  public static void requireEnabled() {
    PolicyEditAuthorization scope = CURRENT.get();
    if (scope != null) {
      scope.requireEnabled = true;
    }
  }

  /** Records that the edit was authorized on the condition that the policy is unbound. */
  public static void requireUnbound() {
    PolicyEditAuthorization scope = CURRENT.get();
    if (scope != null) {
      scope.requireUnbound = true;
    }
  }

  /**
   * Returns whether the named policy must still be unbound when it is written.
   *
   * @param identifier the policy being updated
   * @return whether the write must validate the binding state
   */
  public static boolean requiresUnbound(NameIdentifier identifier) {
    PolicyEditAuthorization scope = CURRENT.get();
    return scope != null
        && scope.requireUnbound
        && scope.checked != null
        && scope.checked.nameIdentifier().equals(identifier);
  }

  /**
   * Returns whether authorization recorded this policy in the current request.
   *
   * @param identifier the policy identifier
   * @return whether a policy snapshot was recorded
   */
  public static boolean isChecked(NameIdentifier identifier) {
    PolicyEditAuthorization scope = CURRENT.get();
    return scope != null
        && scope.checked != null
        && scope.checked.nameIdentifier().equals(identifier);
  }

  /**
   * Rejects a write when the stored policy has changed in a way that invalidates authorization.
   *
   * @param identifier the policy being updated
   * @param current the policy read by the update operation
   */
  public static void check(NameIdentifier identifier, PolicyEntity current) {
    PolicyEditAuthorization scope = CURRENT.get();
    PolicyEntity checked = scope == null ? null : scope.checked;
    if (checked != null
        && checked.nameIdentifier().equals(identifier)
        && (!Objects.equals(checked.id(), current.id())
            || checked.policyType() != current.policyType()
            || !Objects.equals(checked.content(), current.content())
            || (scope.requireEnabled && !current.enabled()))) {
      throw new OptimisticLockException(
          "The policy %s changed after authorization; retry the operation", identifier);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    if (previous == null) {
      CURRENT.remove();
    } else {
      CURRENT.set(previous);
    }
  }
}
