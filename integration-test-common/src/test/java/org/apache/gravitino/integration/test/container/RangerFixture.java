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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Declares that a test class needs the Ranger admin server. Test and lifecycle methods receive it
 * as a {@link RangerContainer} parameter.
 *
 * <pre>{@code
 * @RangerFixture
 * class RangerExampleIT {
 *   @BeforeAll
 *   static void setup(RangerContainer ranger) { ... }
 * }
 * }</pre>
 */
@Documented
@Inherited
@Target({ElementType.TYPE, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(ContainerExtension.class)
@ContainerFixture(RangerSpec.Factory.class)
public @interface RangerFixture {

  /**
   * Returns how long the container lives. Use {@link Scope#CLASS} or {@link Scope#METHOD} for tests
   * that change the service in ways later tests would see, or that need durable data.
   *
   * @return the scope; the whole run by default, shared with other classes
   */
  Scope scope() default Scope.RUN;

  /**
   * Returns when the container starts.
   *
   * @return the startup mode; before {@code @BeforeAll} by default
   */
  Startup startup() default Startup.EAGER;

  /**
   * Returns whether the container keeps its durability work. A durable container is private to the
   * class or test, so it needs {@code scope = Scope.CLASS} or {@code Scope.METHOD}.
   *
   * @return disposable by default: data on {@code tmpfs} and no syncing, where the service allows
   */
  Durability durability() default Durability.DISPOSABLE;
}
