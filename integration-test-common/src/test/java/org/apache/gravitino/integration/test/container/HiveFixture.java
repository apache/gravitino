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
 * Declares that a test class needs a Hive server. Test and lifecycle methods receive it as a {@link
 * HiveContainer} parameter, and fields marked {@link FixtureContainer} receive it too.
 *
 * <p>A class gets one Hive: the annotation is not repeatable, and both authentication modes inject
 * as {@link HiveContainer}. A subclass may declare its own {@code HiveFixture}; it replaces the
 * superclass's.
 */
@Documented
@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(ContainerExtension.class)
@ContainerFixture(HiveSpec.Factory.class)
public @interface HiveFixture {

  /**
   * Returns the Hive version to run.
   *
   * @return the Hive version; Hive2 by default
   */
  HiveVersion version() default HiveVersion.HIVE2;

  /**
   * Returns how clients authenticate to Hive.
   *
   * @return the authentication mode; none by default
   */
  HiveAuth auth() default HiveAuth.NONE;

  /**
   * Returns how long the Hive container lives. Use {@link Scope#CLASS} or {@link Scope#METHOD} for
   * tests that change Hive in ways later tests would see, such as adding Kerberos principals.
   *
   * @return the scope; the whole run by default, shared with other classes
   */
  Scope scope() default Scope.RUN;
}
