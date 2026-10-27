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

/**
 * Whether a test container keeps the durability work its server normally does. No test reads data
 * back after a container stops, so containers are {@link #DISPOSABLE} unless a fixture asks for
 * {@link #DURABLE}.
 */
public enum Durability {
  /**
   * Data on {@code tmpfs} and no syncing to disk, where the service allows it; each container class
   * says how in {@link BaseContainer#applyDisposableData}.
   */
  DISPOSABLE,
  /** The image's own settings: data on disk, synced as the server normally does. */
  DURABLE;

  /**
   * System property that, when {@code true}, makes every container {@link #DURABLE}, for example to
   * inspect a container's data after a failure. Gradle sets it with {@code
   * -PdurableTestContainers}.
   */
  public static final String DURABLE_CONTAINERS_PROPERTY = "gravitino.test.durableContainers";

  /**
   * Returns the durability of a container no fixture chose one for.
   *
   * @return {@link #DURABLE} when {@link #DURABLE_CONTAINERS_PROPERTY} is set, else {@link
   *     #DISPOSABLE}
   */
  public static Durability runDefault() {
    return Boolean.getBoolean(DURABLE_CONTAINERS_PROPERTY) ? DURABLE : DISPOSABLE;
  }
}
