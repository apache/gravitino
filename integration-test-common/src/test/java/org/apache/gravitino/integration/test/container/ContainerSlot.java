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

import java.util.Objects;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * Holds at most one running container, together with the key it was started for.
 *
 * <p>A request with the running key returns the running container. A request with another key stops
 * the running one first, so a slot holds one container at a time: its keys are variants of one
 * heavy service, such as Hive versions, that tests never need together and that would not fit side
 * by side on a CI runner.
 *
 * @param <K> what makes two requests different, such as a Hive version
 * @param <C> the container type
 */
final class ContainerSlot<K, C extends BaseContainer> {
  @Nullable private volatile K key;
  @Nullable private volatile C container;

  synchronized C getOrStart(K requestedKey, Supplier<C> starter) {
    if (container != null && Objects.equals(key, requestedKey)) {
      return container;
    }
    if (container != null) {
      BaseContainer.LOG.info(
          "Replacing {} {} with {}", container.getClass().getSimpleName(), key, requestedKey);
      container.close();
      clear();
    }
    C started = starter.get();
    container = started;
    key = requestedKey;
    return started;
  }

  @Nullable
  C get() {
    return container;
  }

  synchronized void clear() {
    container = null;
    key = null;
  }
}
