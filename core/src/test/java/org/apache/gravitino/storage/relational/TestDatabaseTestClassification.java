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

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.reflect.ClassPath;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.support.AnnotationSupport;

/**
 * Structurally guards the two failure modes the old per-backend exclude-tag scheme allowed
 * silently: a class excluded from every database lane (an empty {@code backends()}), and a class
 * still using the raw {@code @Tag} strings {@link DatabaseTest} replaced instead of the annotation
 * itself. Runs in {@code coreUnitTest} on every build - no database, no Docker.
 *
 * <p>Class-level only: lane membership (which this checks) is a property of the class, not of
 * individual {@code @ParameterizedTest}/{@code @TestTemplate} invocations, so this sidesteps the
 * static-discovery limitations of trying to enumerate invocations ahead of execution.
 */
class TestDatabaseTestClassification {

  private static final Set<String> LEGACY_BACKEND_TAGS =
      Set.of(
          "gravitino-core-h2-test", "gravitino-core-mysql-test", "gravitino-core-postgresql-test");

  @Test
  void everyDatabaseTestDeclaresAtLeastOneBackendAndNoClassUsesTheRetiredRawTags()
      throws IOException {
    List<String> emptyBackends = new ArrayList<>();
    List<String> legacyRawTags = new ArrayList<>();
    List<String> rawDatabaseTag = new ArrayList<>();

    for (ClassPath.ClassInfo info :
        ClassPath.from(getClass().getClassLoader())
            .getTopLevelClassesRecursive("org.apache.gravitino")) {
      Class<?> candidate;
      try {
        candidate = info.load();
      } catch (LinkageError e) {
        // A class that can't even be linked on this classpath can't be a test class we care
        // about; skip it rather than fail the whole scan.
        continue;
      }
      if (candidate.isInterface() || Modifier.isAbstract(candidate.getModifiers())) {
        continue;
      }

      AnnotationSupport.findAnnotation(candidate, DatabaseTest.class)
          .ifPresent(
              annotation -> {
                if (annotation.backends().length == 0) {
                  emptyBackends.add(candidate.getName());
                }
              });

      for (Annotation direct : candidate.getDeclaredAnnotations()) {
        if (!(direct instanceof Tag)) {
          continue;
        }
        String value = ((Tag) direct).value();
        if (LEGACY_BACKEND_TAGS.contains(value)) {
          legacyRawTags.add(candidate.getName() + " -> " + value);
        } else if (DatabaseTest.TAG.equals(value)) {
          rawDatabaseTag.add(candidate.getName());
        }
      }
    }

    assertTrue(
        emptyBackends.isEmpty(),
        "@DatabaseTest(backends = {}) declares a class applicable to no backend, which is "
            + "excluded from every database lane - the exact silent failure mode this check "
            + "exists to catch:\n"
            + String.join("\n", emptyBackends));

    assertTrue(
        legacyRawTags.isEmpty(),
        "These classes still use the retired per-backend @Tag strings directly; use "
            + "@DatabaseTest(backends = ...) instead so BackendLaneCondition enforces the "
            + "restriction:\n"
            + String.join("\n", legacyRawTags));

    assertTrue(
        rawDatabaseTag.isEmpty(),
        "These classes declare @Tag(\""
            + DatabaseTest.TAG
            + "\") directly instead of @DatabaseTest; use @DatabaseTest so backend restriction "
            + "is enforced consistently:\n"
            + String.join("\n", rawDatabaseTag));
  }
}
