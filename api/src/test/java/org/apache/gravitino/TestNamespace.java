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
package org.apache.gravitino;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestNamespace {

  @Test
  public void testEmptyNamespace() {
    Namespace ns = Namespace.empty();

    Assertions.assertEquals(0, ns.length());
    Assertions.assertArrayEquals(new String[0], ns.levels());
    Assertions.assertEquals(true, ns.isEmpty());
  }

  @Test
  public void testCreateNamespace() {
    Namespace ns = Namespace.of("a", "b", "c");

    Assertions.assertEquals(3, ns.length());
    Assertions.assertArrayEquals(new String[] {"a", "b", "c"}, ns.levels());
    Assertions.assertEquals("a", ns.level(0));
    Assertions.assertThrows(IllegalArgumentException.class, () -> ns.level(3));
    Assertions.assertEquals(false, ns.isEmpty());

    // Test namespace with null or empty levels
    Assertions.assertThrows(IllegalArgumentException.class, () -> Namespace.of("a", null, "c"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> Namespace.of("a", "", "c"));
  }
}
