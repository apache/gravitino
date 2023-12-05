/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datastrato.gravitino.server.web;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.function.Supplier;

/**
 * Convenience class to ensure that a new Thread is created inside a privileged block.
 *
 * <p>This prevents the Thread constructor from pinning the caller's context classloader. This
 * happens when the Thread constructor takes a snapshot of the current calling context - which
 * contains ProtectionDomains that may reference the context classloader - and remembers it for the
 * lifetime of the Thread.
 *
 * <p>Referred from org/eclipse/jetty/util/thread/PrivilegedThreadFactory.java;
 */
class PrivilegedThreadFactory {
  /**
   * Use a Supplier to make a new thread, calling it within a privileged block to prevent
   * classloader pinning.
   *
   * @param newThreadSupplier a Supplier to create a fresh thread
   * @return a new thread, protected from classloader pinning.
   */
  static <T extends Thread> T newThread(Supplier<T> newThreadSupplier) {
    return AccessController.doPrivileged(
        new PrivilegedAction<T>() {
          @Override
          public T run() {
            return newThreadSupplier.get();
          }
        });
  }
}
