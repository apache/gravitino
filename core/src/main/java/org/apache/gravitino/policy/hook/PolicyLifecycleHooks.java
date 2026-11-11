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
package org.apache.gravitino.policy.hook;

import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Holds the optional {@link PolicyLifecycleHook} registered by an extension.
 *
 * <p>This is a storage-layer SPI invoked from {@code PolicyMetaService} inside the open policy
 * transaction. It is not the same as {@code org.apache.gravitino.hook.PolicyHookDispatcher}, which
 * wraps the policy API for owner side effects after a successful manager call.
 */
public final class PolicyLifecycleHooks {

  private static final AtomicReference<PolicyLifecycleHook> HOOK = new AtomicReference<>();

  private PolicyLifecycleHooks() {}

  /**
   * Installs the hook invoked at policy create / update / delete lifecycle points inside the open
   * transaction.
   *
   * @param hook hook to run during policy create, update, and delete transactions
   * @throws IllegalStateException if a hook is already registered
   */
  public static void register(PolicyLifecycleHook hook) {
    if (!HOOK.compareAndSet(null, hook)) {
      throw new IllegalStateException("A policy lifecycle hook is already registered");
    }
  }

  /**
   * Removes the hook when it is the one currently registered.
   *
   * @param hook hook passed to {@link #register}
   */
  public static void unregister(PolicyLifecycleHook hook) {
    HOOK.compareAndSet(hook, null);
  }

  /**
   * @return the registered hook, or {@code null} when no extension is participating
   */
  @Nullable
  public static PolicyLifecycleHook get() {
    return HOOK.get();
  }
}
