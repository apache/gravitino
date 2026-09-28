/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.storage.relational;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.Tag;

/**
 * Marks a core database test class that runs under every JDBC backend lane.
 *
 * <p>The core test suite is split into one Gradle task per backend ({@code coreH2Test}, {@code
 * coreMySQLTest}, {@code corePostgreSQLTest}), and each lane simply includes the classes carrying
 * its backend tag. A class therefore runs under exactly the backends it is tagged with:
 *
 * <ul>
 *   <li>{@code @Tag("gravitino-core-mysql-test")} pins a class to the MySQL lane; list several tags
 *       to run under several lanes.
 *   <li>{@code @AllBackendsTest} runs a class under all three lanes.
 *   <li>A class with none of the backend tags is a unit test and runs in {@code coreUnitTest}.
 * </ul>
 *
 * <p>This is a plain JUnit composed annotation: it is meta-annotated with one {@link Tag} per
 * backend and nothing else, so it is exactly equivalent to writing the three {@code @Tag}s on the
 * class. JUnit expands it while scanning class annotations during test discovery (see {@code
 * org.junit.platform.commons.support.AnnotationSupport#findRepeatableAnnotations}), so {@code
 * core/build.gradle.kts} only needs the three backend tag strings and no execution-time condition
 * is involved. The tag strings must stay in sync with {@code coreBackendTestTags} in {@code
 * core/build.gradle.kts}.
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Tag("gravitino-core-h2-test")
@Tag("gravitino-core-mysql-test")
@Tag("gravitino-core-postgresql-test")
public @interface AllBackendsTest {}
