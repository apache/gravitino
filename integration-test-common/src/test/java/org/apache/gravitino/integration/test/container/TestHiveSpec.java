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
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/** Tests the Kerberos principals of {@link HiveSpec} and {@link HiveFixture} without Docker. */
public class TestHiveSpec {

  @Test
  void testPrincipalsAreSortedSoThatTheirOrderMakesNoNewContainer() {
    HiveSpec spec = kerberos(List.of("b", "a", "b"));

    Assertions.assertEquals(List.of("a", "b"), spec.kerberosPrincipals());
    Assertions.assertEquals(kerberos(List.of("a", "b")), spec);
  }

  @Test
  void testDifferentPrincipalsAreDifferentContainers() {
    Assertions.assertNotEquals(kerberos(List.of("a")), kerberos(List.of("b")));
    Assertions.assertNotEquals(kerberos(List.of("a")), kerberos(List.of()));
    Assertions.assertEquals(
        new HiveSpec(HiveVersion.HIVE2, HiveAuth.KERBEROS), kerberos(List.of()));
  }

  @Test
  void testPrincipalsNeedKerberos() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new HiveSpec(HiveVersion.HIVE2, HiveAuth.NONE, HiveStorage.HDFS, List.of("a")));
  }

  @Test
  void testPrincipalsAreBareNames() {
    for (String name : List.of("a@HADOOPKRB", "hdfs/host", "a b", "a'b", "")) {
      Assertions.assertThrows(IllegalArgumentException.class, () -> kerberos(List.of(name)), name);
    }
    Assertions.assertDoesNotThrow(() -> kerberos(List.of("cli_schema", "svc-1.test")));
  }

  @Test
  void testFixtureMapsItsPrincipals() {
    Assertions.assertEquals(
        List.of(kerberos(List.of("cli_fileset", "cli_schema"))),
        ContainerExtension.specs(ClassScopedPrincipals.class));
  }

  @Test
  void testFixtureWithPrincipalsNeedsAContainerOfItsOwn() {
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> ContainerExtension.specs(RunScopedPrincipals.class));
    Assertions.assertTrue(e.getMessage().contains("Scope.CLASS"), e.getMessage());
  }

  @Test
  void testSharedStartRefusesPrincipals() {
    SharedContainers shared = Mockito.mock(SharedContainers.class);

    Assertions.assertThrows(
        IllegalStateException.class, () -> kerberos(List.of("a")).getOrStart(shared));
    Mockito.verifyNoInteractions(shared);
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void testPrivateStartPassesThePrincipalsToTheBuilder() {
    SharedContainers shared = Mockito.mock(SharedContainers.class);

    kerberos(List.of("cli_schema")).startPrivate(shared, Durability.DISPOSABLE);

    ArgumentCaptor<BaseContainer.Builder> builder =
        ArgumentCaptor.forClass(BaseContainer.Builder.class);
    Mockito.verify(shared).startPrivate(builder.capture(), Mockito.eq(Durability.DISPOSABLE));
    HiveContainer.Builder hive = (HiveContainer.Builder) builder.getValue();
    Assertions.assertEquals("gravitino-ci-kerberos-hive", hive.hostName);
    Assertions.assertEquals(List.of("cli_schema"), hive.kerberosPrincipals);
  }

  private static HiveSpec kerberos(List<String> principals) {
    return new HiveSpec(HiveVersion.HIVE2, HiveAuth.KERBEROS, HiveStorage.HDFS, principals);
  }

  @HiveFixture(
      auth = HiveAuth.KERBEROS,
      scope = Scope.CLASS,
      kerberosPrincipals = {"cli_schema", "cli_fileset"})
  static class ClassScopedPrincipals {}

  @HiveFixture(auth = HiveAuth.KERBEROS, kerberosPrincipals = "cli_schema")
  static class RunScopedPrincipals {}
}
