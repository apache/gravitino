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
 * Annotations that choose which core database test lane(s) a class runs in. Each lane task ({@code
 * coreH2Test}, {@code coreMySQLTest}, {@code corePostgreSQLTest}) includes only classes with its
 * tag; stack annotations to join several lanes, or use {@link All}. A class with none of them is a
 * unit test and runs in {@code coreUnitTest}. {@code TestCoreLaneMembership} fails if a class
 * carries {@code gravitino-docker-test} but no backend tag, since it would run in no lane.
 *
 * <p>The lane, not the class, decides whether Docker is needed: MySQL and PostgreSQL require it and
 * H2 does not, so a class tagged for H2 must not need a container itself. The tag strings must stay
 * in sync with {@code coreBackendTestTags} in {@code core/build.gradle.kts}.
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
