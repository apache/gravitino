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
   * Returns the running container for this spec, starting it on first use.
   *
   * <p>{@link ContainerSuite} keeps the container for the whole run, so tests that use fixtures and
   * tests that call {@code ContainerSuite} directly share it.
   *
   * @param suite the shared container suite
   * @return the running container
   */
  C getOrStart(ContainerSuite suite);
}
