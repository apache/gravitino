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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

/** Tests for {@link SharedContainers}. */
public class TestSharedContainers {

  @Test
  void testCloseResetsEveryContainerReference() throws Exception {
    // Every mutable static field, so a field added later without a reset fails this test.
    List<Field> stateFields =
        Arrays.stream(SharedContainers.class.getDeclaredFields())
            .filter(f -> Modifier.isStatic(f.getModifiers()))
            .filter(f -> !Modifier.isFinal(f.getModifiers()))
            .filter(f -> !f.getName().equals("instance"))
            .toList();
    Assertions.assertFalse(stateFields.isEmpty());
    for (Field field : stateFields) {
      field.setAccessible(true);
      markStarted(field);
    }

    HiveContainer hive = Mockito.mock(HiveContainer.class);
    hiveSlot("hiveContainer").getOrStart(HiveContainer.HIVE3, () -> hive);
    hiveSlot("kerberosHiveContainer").getOrStart(HiveContainer.HIVE3, () -> hive);

    SharedContainers.getInstance().close();

    Assertions.assertNull(SharedContainers.getInstance().getHiveContainer());
    Assertions.assertNull(SharedContainers.getInstance().getKerberosHiveContainer());
    for (Field field : stateFields) {
      Object value = field.get(null);
      if (value instanceof Map) {
        Assertions.assertTrue(((Map<?, ?>) value).isEmpty(), field.getName());
      } else if (value instanceof Collection) {
        Assertions.assertTrue(((Collection<?>) value).isEmpty(), field.getName());
      } else if (field.getType() == boolean.class) {
        Assertions.assertFalse((boolean) value, field.getName());
      } else {
        Assertions.assertNull(value, field.getName());
      }
    }
  }

  @Test
  void testHiveVersionDefaultsToHive2() {
    Assertions.assertEquals(HiveContainer.HIVE2, SharedContainers.hiveVersion(Map.of()));
    Assertions.assertEquals(
        HiveContainer.HIVE3,
        SharedContainers.hiveVersion(
            Map.of(HiveContainer.HIVE_RUNTIME_VERSION, HiveContainer.HIVE3)));
  }

  @Test
  void testStartTimedStartsTheContainer() {
    BaseContainer container = mockContainer();

    SharedContainers.startTimed(container);

    Mockito.verify(container).start();
  }

  @Test
  void testStartTimedKeepsAStartedContainerRunning() {
    BaseContainer container = mockContainer();

    SharedContainers.startTimed(container);

    Mockito.verify(container, Mockito.never()).close();
  }

  @Test
  void testStartTimedClosesAndRethrowsFailedStart() {
    BaseContainer container = mockContainer();
    IllegalStateException failure = new IllegalStateException("not ready");
    Mockito.doThrow(failure).when(container).start();

    Assertions.assertSame(
        failure,
        Assertions.assertThrows(
            IllegalStateException.class, () -> SharedContainers.startTimed(container)));
    Mockito.verify(container).close();
  }

  @Test
  void testStartTimedReportsStartFailureWhenCloseFails() {
    BaseContainer container = mockContainer();
    IllegalStateException failure = new IllegalStateException("not ready");
    IllegalStateException closeFailure = new IllegalStateException("cannot stop");
    Mockito.doThrow(failure).when(container).start();
    Mockito.doThrow(closeFailure).when(container).close();

    IllegalStateException thrown =
        Assertions.assertThrows(
            IllegalStateException.class, () -> SharedContainers.startTimed(container));

    Assertions.assertSame(failure, thrown);
    Assertions.assertArrayEquals(new Throwable[] {closeFailure}, thrown.getSuppressed());
  }

  @Test
  void testStartTimedReportsStartFailureWhenCloseThrowsAnError() {
    BaseContainer container = mockContainer();
    IllegalStateException failure = new IllegalStateException("not ready");
    AssertionError closeFailure = new AssertionError("cannot stop");
    Mockito.doThrow(failure).when(container).start();
    Mockito.doThrow(closeFailure).when(container).close();

    IllegalStateException thrown =
        Assertions.assertThrows(
            IllegalStateException.class, () -> SharedContainers.startTimed(container));

    Assertions.assertSame(failure, thrown);
    Assertions.assertArrayEquals(new Throwable[] {closeFailure}, thrown.getSuppressed());
  }

  @Test
  void testStartPrivateStartsAndRegistersWithoutKeepingTheContainer() throws Exception {
    BaseContainer container = mockContainer();

    BaseContainer started =
        SharedContainers.startPrivate(
            null, network -> container, SharedContainers::startTimed, running -> {});

    Assertions.assertSame(container, started);
    Mockito.verify(container, Mockito.times(1)).start();
    for (Field field : SharedContainers.class.getDeclaredFields()) {
      if (!Modifier.isStatic(field.getModifiers()) || field.getName().equals("closer")) {
        continue;
      }
      field.setAccessible(true);
      Object value = field.get(null);
      Assertions.assertNotSame(container, value, field.getName());
      if (value instanceof Map) {
        Assertions.assertFalse(((Map<?, ?>) value).containsValue(container), field.getName());
      } else if (value instanceof Collection) {
        Assertions.assertFalse(((Collection<?>) value).contains(container), field.getName());
      }
    }

    // Registered with the closer, so the end of the run closes it even if the caller does not.
    SharedContainers.getInstance().close();
    Mockito.verify(container).close();
  }

  @Test
  void testFailedPrivateSetUpClosesTheContainerAndRethrows() {
    BaseContainer container = mockContainer();
    IllegalStateException failure = new IllegalStateException("cannot create the database");

    Assertions.assertSame(
        failure,
        Assertions.assertThrows(
            IllegalStateException.class,
            () ->
                SharedContainers.startPrivate(
                    null,
                    network -> container,
                    SharedContainers::startTimed,
                    started -> {
                      throw failure;
                    })));
    Mockito.verify(container).close();
  }

  @Test
  void testPrivateBuildUsesTheNetworkAndTheDurability() {
    Network network = Mockito.mock(Network.class);

    ZooKeeperContainer container =
        SharedContainers.privateBuild(SharedContainers.zooKeeperBuilder(), Durability.DURABLE)
            .apply(network);

    Assertions.assertEquals(Durability.DURABLE, container.durability());
    Assertions.assertSame(network, container.getContainer().getNetwork());
    Assertions.assertTrue(
        container.getContainer().getNetworkAliases().contains(ZooKeeperContainer.HOST_NAME));
  }

  private static BaseContainer mockContainer() {
    GenericContainer<?> genericContainer = Mockito.mock(GenericContainer.class);
    Mockito.when(genericContainer.getDockerImageName()).thenReturn("test/image:1.0");
    BaseContainer container = Mockito.mock(BaseContainer.class);
    Mockito.doReturn(genericContainer).when(container).getContainer();
    return container;
  }

  @SuppressWarnings("unchecked")
  private static ContainerSlot<String, HiveContainer> hiveSlot(String fieldName) throws Exception {
    Field field = SharedContainers.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return (ContainerSlot<String, HiveContainer>) field.get(null);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void markStarted(Field field) throws IllegalAccessException {
    Class<?> type = field.getType();
    if (type == boolean.class) {
      field.setBoolean(null, true);
    } else if (Map.class.isAssignableFrom(type)) {
      // The maps are EnumMaps, so the key must be a constant of the declared key enum.
      Class<?> keyType =
          (Class<?>) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
      Class<?> valueType =
          (Class<?>) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[1];
      ((Map) field.get(null)).put(keyType.getEnumConstants()[0], Mockito.mock(valueType));
    } else if (List.class.isAssignableFrom(type)) {
      Class<?> elementType =
          (Class<?>) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
      field.set(null, List.of(Mockito.mock(elementType)));
    } else {
      field.set(null, Mockito.mock(type));
    }
  }
}
