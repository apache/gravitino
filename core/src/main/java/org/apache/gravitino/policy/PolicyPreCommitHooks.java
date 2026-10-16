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

import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/** Holds the optional {@link PolicyPreCommitHook} registered by an extension. */
public final class PolicyPreCommitHooks {

  private static final AtomicReference<PolicyPreCommitHook> HOOK = new AtomicReference<>();

  private PolicyPreCommitHooks() {}

  /**
   * Installs the hook used by policy transactions.
   *
   * @param hook hook to run before a policy transaction commits
   * @throws IllegalStateException if a hook is already registered
   */
  public static void register(PolicyPreCommitHook hook) {
    if (!HOOK.compareAndSet(null, hook)) {
      throw new IllegalStateException("A policy pre-commit hook is already registered");
    }
  }

  /**
   * Removes the hook when it is the one currently registered.
   *
   * @param hook hook passed to {@link #register}
   */
  public static void unregister(PolicyPreCommitHook hook) {
    HOOK.compareAndSet(hook, null);
  }

  /**
   * @return the registered hook, or {@code null} when no extension is participating
   */
  @Nullable
  public static PolicyPreCommitHook get() {
    return HOOK.get();
  }
}
