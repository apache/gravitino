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

/**
 * What a test asks for: one kind of container in one configuration.
 *
 * <p>Two equal specs mean the same container, so a spec is a record of everything that makes two
 * requests different, such as a version. A service with nothing to choose has an empty spec.
 *
 * @param <C> the container type a test receives
 */
public interface ContainerSpec<C extends BaseContainer> {

  /**
   * Returns the container type a test receives for this spec.
   *
   * @return the container type
   */
  Class<C> containerType();

  /**
   * Returns whether a field or parameter of the given type can receive this spec's container. By
   * default it can when the type is the container type or a supertype of it; a spec may offer more
   * types, such as a view of several containers.
   *
   * @param wanted the type of the field or parameter
   * @return whether this spec supplies that type
   */
  default boolean supplies(Class<?> wanted) {
    return wanted.isAssignableFrom(containerType());
  }

  /**
   * Returns what a field or parameter of a type this spec {@linkplain #supplies(Class) supplies}
   * receives for the running container.
   *
   * @param wanted the type of the field or parameter
   * @param container the running container
   * @return the container itself by default
   */
  default Object injectAs(Class<?> wanted, C container) {
    return container;
  }

  /**
   * Returns the specs whose containers must run before this one starts, such as the Ranger server a
   * Ranger-enabled Hive reports to. They start first, as shared run-wide containers, so they
   * outlive this container whatever its scope. Inside {@link #getOrStart} the spec reads a
   * dependency's running container with {@code dependency.getOrStart(shared)}, which returns the
   * one already started.
   *
   * @return the direct dependencies; none by default
   */
  default List<ContainerSpec<?>> dependencies() {
    return List.of();
  }

  /**
   * Returns this spec completed with what the test instance provides, such as the Gravitino
   * server's port. The extension calls it just before it starts the container.
   *
   * @param testInstance the test instance
   * @return this spec by default
   */
  default ContainerSpec<C> forTestInstance(Object testInstance) {
    return this;
  }

  /**
   * Returns the running shared container for this spec, starting it on first use. The container
   * lives for the whole run, so every fixture and every {@link Fixtures#get} call for an equal spec
   * receives it. Only this package calls it; other code calls {@link Fixtures#get}, which starts
   * the dependencies first.
   *
   * @param shared the run's shared containers
   * @return the running container
   */
  C getOrStart(SharedContainers shared);

  /**
   * Returns whether {@link #startPrivate} can start a container for this spec, so that a fixture
   * asking for {@link Scope#CLASS} or {@link Scope#METHOD} fails when its class is found rather
   * than when the container starts.
   *
   * @return false unless the spec overrides {@link #startPrivate}
   */
  default boolean supportsPrivate() {
    return false;
  }

  /**
   * Returns why this spec has no private containers, for the error a fixture that asks for one
   * gets. Read only when {@link #supportsPrivate()} is false.
   *
   * @return the reason, as a clause that follows the spec's name
   */
  default String noPrivateReason() {
    return "has no private containers";
  }

  /**
   * Starts a new container for this spec that only the caller uses, for fixtures with {@link
   * Scope#CLASS} or {@link Scope#METHOD}. It is built like the shared container, on the run's
   * network, and starts through the same timed path; it never replaces the shared container. The
   * caller closes it. Its {@linkplain #dependencies() dependencies} stay shared unless the spec
   * says otherwise.
   *
   * @param shared the run's shared containers, which start the container on the run's network
   * @param durability whether the container keeps its durability work
   * @return the running private container
   * @throws UnsupportedOperationException if this spec has no private containers
   */
  default C startPrivate(SharedContainers shared, Durability durability) {
    throw new UnsupportedOperationException(getClass().getSimpleName() + " " + noPrivateReason());
  }
}
