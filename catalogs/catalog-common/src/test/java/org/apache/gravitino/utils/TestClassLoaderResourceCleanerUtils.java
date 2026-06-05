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

package org.apache.gravitino.utils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.net.URLClassLoader;
import org.junit.jupiter.api.Test;

class TestClassLoaderResourceCleanerUtils {

  /**
   * When a class is loaded by exactly the target classloader, isOwnedByClassLoader must return true
   * — the guard should allow static-state cleanup to proceed.
   */
  @Test
  void testIsOwnedByClassLoaderReturnsTrueForOwningLoader() {
    ClassLoader loader = ClassLoaderResourceCleanerUtils.class.getClassLoader();
    assertTrue(
        ClassLoaderResourceCleanerUtils.isOwnedByClassLoader(
            ClassLoaderResourceCleanerUtils.class, loader));
  }

  /**
   * When a class was resolved via parent delegation (i.e. the actual loader is the parent, not the
   * child), isOwnedByClassLoader must return false — the guard should skip cleanup to avoid
   * mutating shared JVM-global static state.
   */
  @Test
  void testIsOwnedByClassLoaderReturnsFalseForParentDelegatedClass() throws Exception {
    ClassLoader parent = ClassLoaderResourceCleanerUtils.class.getClassLoader();
    // Child delegates everything to the parent; ClassLoaderResourceCleanerUtils is therefore
    // parent-loaded, not child-loaded.
    try (URLClassLoader child = new URLClassLoader(new URL[0], parent)) {
      assertFalse(
          ClassLoaderResourceCleanerUtils.isOwnedByClassLoader(
              ClassLoaderResourceCleanerUtils.class, child));
    }
  }

  /**
   * Bootstrap-loaded classes (whose getClassLoader() returns null) are never "owned" by a named
   * classloader — the guard must return false for them too.
   */
  @Test
  void testIsOwnedByClassLoaderReturnsFalseForBootstrapLoadedClass() {
    // String is loaded by the bootstrap classloader; getClassLoader() returns null.
    assertFalse(
        ClassLoaderResourceCleanerUtils.isOwnedByClassLoader(
            String.class, ClassLoader.getSystemClassLoader()));
  }
}
