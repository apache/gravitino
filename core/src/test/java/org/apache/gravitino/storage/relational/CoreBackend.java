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
 * Namespace for the annotations that select which core database test lane(s) a class runs in.
 *
 * <p>The core test suite is split into one Gradle task per backend ({@code coreH2Test}, {@code
 * coreMySQLTest}, {@code corePostgreSQLTest}), and each lane simply includes the classes carrying
 * its backend tag. A class runs under exactly the lanes it is tagged for - there is no separate "is
 * this a database test" gatekeeper tag to keep in sync:
 *
 * <ul>
 *   <li>{@link H2}, {@link MySQL}, {@link PostgreSQL} pin a class to one lane; stack more than one
 *       to run under several lanes, e.g. {@code @CoreBackend.H2 @CoreBackend.MySQL}. A class tagged
 *       for some but not all backends must satisfy the CI-legality constraint below.
 *   <li>{@link All} runs a class under all three lanes.
 *   <li>A class with none of these is a unit test and runs in {@code coreUnitTest} - unless it also
 *       carries {@code @Tag("gravitino-docker-test")}, in which case it runs in no lane at all.
 *       Check with {@code ./gradlew :core:coreTestLaneOf -PclassName=<fully.qualified.ClassName>}.
 * </ul>
 *
 * <p>Each is a plain JUnit composed annotation: meta-annotated with {@link Tag} and nothing else,
 * so it is exactly equivalent to writing the raw {@code @Tag} string(s) on the class. JUnit expands
 * it while scanning class annotations during test discovery (see {@code
 * org.junit.platform.commons.support.AnnotationSupport#findRepeatableAnnotations}), so {@code
 * core/build.gradle.kts} only needs the tag strings themselves and no execution-time condition is
 * involved. {@link #H2_TAG}, {@link #MYSQL_TAG}, and {@link #POSTGRESQL_TAG} must stay in sync with
 * {@code coreBackendTestTags} in {@code core/build.gradle.kts}.
 *
 * <p>Because {@code dev/ci/core_test_identity.py}'s {@code reconcile} step (invoked from {@code
 * .github/workflows/build.yml}, not from {@code core/build.gradle.kts}) requires the
 * h2/mysql/postgresql lanes to run the exact same set of normalized test identities, a single- or
 * multi- (but not all-) backend class is only CI-legal as a normalized sibling of matching classes
 * in the other backend(s) it omits - see {@code TestJdbcPartitionStatisticStorageIT}'s {@code
 * H2Test}/{@code MySQLTest}/{@code PostgreSQLTest} nested classes for the pattern this currently
 * requires.
 */
public final class CoreBackend {

  /** The JUnit tag that selects the H2 lane. */
  public static final String H2_TAG = "gravitino-core-h2-test";

  /** The JUnit tag that selects the MySQL lane. */
  public static final String MYSQL_TAG = "gravitino-core-mysql-test";

  /** The JUnit tag that selects the PostgreSQL lane. */
  public static final String POSTGRESQL_TAG = "gravitino-core-postgresql-test";

  private CoreBackend() {}

  /** Pins a core database test class to the H2 lane ({@code coreH2Test}). */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  @Tag(H2_TAG)
  public @interface H2 {}

  /** Pins a core database test class to the MySQL lane ({@code coreMySQLTest}). */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  @Tag(MYSQL_TAG)
  public @interface MySQL {}

  /** Pins a core database test class to the PostgreSQL lane ({@code corePostgreSQLTest}). */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  @Tag(POSTGRESQL_TAG)
  public @interface PostgreSQL {}

  /** Runs a core database test class under every backend lane. */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  @Tag(H2_TAG)
  @Tag(MYSQL_TAG)
  @Tag(POSTGRESQL_TAG)
  public @interface All {}
}
