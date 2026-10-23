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
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a field into which {@link ContainerExtension} injects a container from the test class's
 * fixtures, such as {@link KafkaFixture}. This is dependency injection: the test declares what it
 * needs, and the extension supplies it, by type, before {@code @BeforeAll} runs.
 *
 * <p>Use it where a method parameter cannot reach, such as an overridden {@code @BeforeAll} method.
 *
 * <pre>{@code
 * @KafkaFixture
 * class OwnerIT extends BaseIT {
 *   @FixtureContainer private KafkaContainer kafka;
 * }
 * }</pre>
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface FixtureContainer {}
