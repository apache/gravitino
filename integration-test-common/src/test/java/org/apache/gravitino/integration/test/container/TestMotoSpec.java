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

public class TestMotoSpec {

  @MotoFixture
  private static class MotoClass {}

  @Test
  void testFactoryReturnsMotoSpec() {
    MotoFixture fixture = MotoClass.class.getAnnotation(MotoFixture.class);
    Assertions.assertEquals(new MotoSpec(), new MotoSpec.Factory().specFor(fixture));
  }

  @Test
  void testContainerTypeIsMotoContainer() {
    Assertions.assertEquals(GravitinoMotoContainer.class, new MotoSpec().containerType());
  }

  @Test
  void testExtensionFindsMotoSpec() {
    Assertions.assertEquals(List.of(new MotoSpec()), ContainerExtension.specs(MotoClass.class));
  }
}
