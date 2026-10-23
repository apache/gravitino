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

import java.lang.annotation.Annotation;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an annotation as a container fixture, such as {@link KafkaFixture}, and names how to turn
 * it into a {@link ContainerSpec}. {@link ContainerExtension} reads every fixture on a test class.
 */
@Documented
@Target(ElementType.ANNOTATION_TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ContainerFixture {

  /**
   * Returns the class that builds the spec from the fixture annotation.
   *
   * @return the spec factory class; it needs a no-argument constructor
   */
  Class<? extends SpecFactory<?>> value();

  /**
   * Builds a {@link ContainerSpec} from a fixture annotation.
   *
   * @param <A> the fixture annotation type
   */
  interface SpecFactory<A extends Annotation> {

    /**
     * Returns the spec the fixture asks for.
     *
     * @param fixture the fixture annotation on the test class
     * @return the spec
     */
    ContainerSpec<?> specFor(A fixture);
  }
}
