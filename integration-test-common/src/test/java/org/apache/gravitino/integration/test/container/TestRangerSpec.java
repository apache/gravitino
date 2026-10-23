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

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

/** Tests {@link RangerSpec} and {@link RangerFixture} without Docker. */
public class TestRangerSpec {

  @Test
  void testFixtureMapsToTheRangerSpec() {
    Assertions.assertEquals(
        List.of(new RangerSpec()), ContainerExtension.specs(RangerFixtureOnly.class));
  }

  @Test
  void testContainerType() {
    Assertions.assertEquals(RangerContainer.class, new RangerSpec().containerType());
  }

  @Test
  void testGetOrStartStartsThroughTheSuite() {
    ContainerSuite suite = Mockito.mock(ContainerSuite.class);
    RangerContainer ranger = Mockito.mock(RangerContainer.class);
    Mockito.when(suite.getRangerContainer()).thenReturn(ranger);

    Assertions.assertSame(ranger, new RangerSpec().getOrStart(suite));

    InOrder order = Mockito.inOrder(suite);
    order.verify(suite).startRangerContainer();
    order.verify(suite).getRangerContainer();
  }

  @RangerFixture
  static class RangerFixtureOnly {}
}
