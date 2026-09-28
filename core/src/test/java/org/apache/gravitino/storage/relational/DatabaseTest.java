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

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Marks a test class as a core database test and declares which backends it applies to.
 *
 * <p>This is the single source of truth for both halves of lane membership that used to be
 * expressed with two disconnected mechanisms: the {@code gravitino-core-database-test} tag (whether
 * {@code core/build.gradle.kts}'s database lanes pick this class up at all, still carried via the
 * meta-annotation below) and, previously, three separate backend-exclusion tags (which of the
 * h2/mysql/postgresql lanes a class actually runs in). {@link #backends()} now answers both
 * questions at once through {@link BackendLaneCondition}, so a class can no longer silently end up
 * excluded from every lane (two conflicting restriction tags) or redundantly re-run in every lane
 * (a missing restriction tag) - both are structural properties of a single annotation instead of
 * independently-maintained tag sets.
 *
 * <p>Omitting {@link #backends()} means "runs under every backend", which is an explicit, visible
 * default rather than something inferred from the absence of any backend-specific tag.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Tag(DatabaseTest.TAG)
@ExtendWith(BackendLaneCondition.class)
public @interface DatabaseTest {

  /** The JUnit tag published for {@code core/build.gradle.kts}'s lane-inclusion filter. */
  String TAG = "gravitino-core-database-test";

  /**
   * The backends this test class applies to. Defaults to every backend.
   *
   * @return the applicable backends
   */
  DatabaseBackend[] backends() default {
    DatabaseBackend.H2, DatabaseBackend.MYSQL, DatabaseBackend.POSTGRESQL
  };
}
