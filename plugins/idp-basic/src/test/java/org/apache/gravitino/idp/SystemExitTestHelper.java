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
package org.apache.gravitino.idp;

/** Test-only helpers for code paths that call {@link System#exit(int)}. */
public final class SystemExitTestHelper {

  private SystemExitTestHelper() {}

  /**
   * Runs an action while intercepting {@link System#exit(int)} and rethrowing it as {@link
   * SystemExitException}.
   *
   * @param action The action that may call {@code System.exit}.
   */
  @SuppressWarnings("removal")
  public static void runWithExitGuard(Runnable action) {
    SecurityManager original = System.getSecurityManager();
    System.setSecurityManager(
        new SecurityManager() {
          @Override
          public void checkExit(int status) {
            throw new SystemExitException(status);
          }

          @Override
          public void checkPermission(java.security.Permission perm) {
            // Allow test execution.
          }
        });
    try {
      action.run();
    } finally {
      System.setSecurityManager(original);
    }
  }

  /** Thrown by {@link #runWithExitGuard(Runnable)} when the guarded action calls {@code exit}. */
  public static final class SystemExitException extends SecurityException {
    private final int status;

    private SystemExitException(int status) {
      super("System.exit(" + status + ")");
      this.status = status;
    }

    /** Returns the exit status passed to {@link System#exit(int)}. */
    public int status() {
      return status;
    }
  }
}
