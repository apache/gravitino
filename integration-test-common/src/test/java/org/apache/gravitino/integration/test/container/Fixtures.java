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

import javax.annotation.Nullable;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;

/**
 * The run's shared containers for code that cannot carry a fixture annotation, such as a test
 * utility, a base class that picks its backend at run time, or a tool outside JUnit.
 *
 * <p>A test class declares its containers with fixture annotations, such as {@link MySQLFixture}.
 * Other code asks for the same {@link ContainerSpec} here and receives the same container a fixture
 * with that spec receives:
 *
 * <pre>{@code
 * MySQLContainer mysql =
 *     Fixtures.get(new MySQLSpec(MySQLVersion.MYSQL_8_0, TestDatabaseName.MYSQL_JDBC_BACKEND));
 * }</pre>
 *
 * <p>The containers close when the JUnit run ends, provided a class of the run uses a fixture or
 * {@code CloseContainerExtension}, as {@code BaseIT} does. Testcontainers removes any others when
 * the JVM exits.
 */
public final class Fixtures {

  private Fixtures() {}

  /**
   * Returns the run's shared container for {@code spec}, starting its dependencies and then the
   * container on first use; the container a {@link Scope#RUN} fixture with this spec receives.
   *
   * @param spec what to start
   * @param <C> the container type
   * @return the running container
   */
  public static <C extends BaseContainer> C get(ContainerSpec<C> spec) {
    return SharedContainers.startWithDependencies(spec, SharedContainers.getInstance());
  }

  /**
   * Returns the network the shared containers run on, for a container a test builds itself that
   * must reach them by host name.
   *
   * @return the network, or null when the run has none: it is created only when {@code
   *     NEED_CREATE_DOCKER_NETWORK=true} (the macOS {@code mac-docker-connector} setup), when the
   *     first shared container starts
   */
  @Nullable
  public static Network network() {
    return SharedContainers.getInstance().getNetwork();
  }

  /**
   * Closes the run's shared containers when the JUnit run ends. Calling it again is harmless.
   * Fixture classes need not call it; {@code CloseContainerExtension} does for the others.
   *
   * @param context any extension context of the run
   */
  public static void closeWhenRunEnds(ExtensionContext context) {
    SharedContainers.closeWhenRunEnds(context);
  }
}
