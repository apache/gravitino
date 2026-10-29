/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.integration.test.container;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Tests {@link ContainerSlot} without Docker. */
public class TestContainerSlot {

  @Test
  void testSameKeyReusesTheRunningContainer() {
    ContainerSlot<String, HiveContainer> slot = new ContainerSlot<>();
    HiveContainer hive2 = Mockito.mock(HiveContainer.class);

    Assertions.assertSame(hive2, slot.getOrStart(HiveContainer.HIVE2, () -> hive2));
    Assertions.assertSame(
        hive2, slot.getOrStart(HiveContainer.HIVE2, () -> Assertions.fail("must not start")));
    Mockito.verify(hive2, Mockito.never()).close();
  }

  @Test
  void testOtherKeyReplacesTheRunningContainer() {
    ContainerSlot<String, HiveContainer> slot = new ContainerSlot<>();
    HiveContainer hive2 = Mockito.mock(HiveContainer.class);
    HiveContainer hive3 = Mockito.mock(HiveContainer.class);
    slot.getOrStart(HiveContainer.HIVE2, () -> hive2);

    Assertions.assertSame(hive3, slot.getOrStart(HiveContainer.HIVE3, () -> hive3));

    Mockito.verify(hive2).close();
    Assertions.assertSame(hive3, slot.get());
  }

  @Test
  void testClearForgetsTheContainer() {
    ContainerSlot<String, HiveContainer> slot = new ContainerSlot<>();
    HiveContainer hive2 = Mockito.mock(HiveContainer.class);
    HiveContainer restarted = Mockito.mock(HiveContainer.class);
    slot.getOrStart(HiveContainer.HIVE2, () -> hive2);

    slot.clear();

    Assertions.assertNull(slot.get());
    Assertions.assertSame(restarted, slot.getOrStart(HiveContainer.HIVE2, () -> restarted));
  }
}
