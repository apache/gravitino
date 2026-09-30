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

import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.annotation.Testable;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.HierarchyTraversalMode;
import org.junit.platform.commons.support.ReflectionSupport;

/**
 * Fails when a core test class runs in no lane: it carries {@code gravitino-docker-test}, which
 * keeps it out of {@code coreUnitTest}, but no {@link CoreBackend} tag, which keeps it out of every
 * database lane. {@code core_test_identity.py reconcile} only compares the database lanes with each
 * other, so a class that leaves all three at once would otherwise go unnoticed.
 */
class TestCoreLaneMembership {

  private static final String DOCKER_TAG = "gravitino-docker-test";
  private static final Set<String> BACKEND_TAGS =
      Set.of(CoreBackend.H2_TAG, CoreBackend.MYSQL_TAG, CoreBackend.POSTGRESQL_TAG);

  @Test
  void everyDockerTaggedTestClassHasABackendLane() {
    URL coreTestClasses = codeLocation(TestCoreLaneMembership.class);
    List<String> orphans =
        ReflectionSupport.findAllClassesInPackage(
                "org.apache.gravitino",
                c -> coreTestClasses.equals(codeLocation(c)) && declaresTests(c),
                name -> true)
            .stream()
            .filter(
                c -> {
                  Set<String> tags = effectiveTags(c);
                  return tags.contains(DOCKER_TAG)
                      && tags.stream().noneMatch(BACKEND_TAGS::contains);
                })
            .map(Class::getName)
            .sorted()
            .collect(Collectors.toList());

    assertTrue(
        orphans.isEmpty(),
        "These classes carry gravitino-docker-test but no @CoreBackend.* tag, so they run in no "
            + "core test lane. Add @CoreBackend.H2/MySQL/PostgreSQL or @CoreBackend.All: "
            + orphans);
  }

  // A class JUnit would run: concrete, with at least one @Test/@ParameterizedTest/... method
  // (all meta-annotated with @Testable), own or inherited. Excludes containers such as
  // TestJdbcPartitionStatisticStorageIT, whose tests live in its static member classes.
  private static boolean declaresTests(Class<?> c) {
    return !Modifier.isAbstract(c.getModifiers())
        && !ReflectionSupport.findMethods(
                c,
                m -> AnnotationSupport.isAnnotated(m, Testable.class),
                HierarchyTraversalMode.TOP_DOWN)
            .isEmpty();
  }

  // Own and inherited tags, plus the enclosing classes' tags for non-static @Nested classes, which
  // JUnit applies to them. Static member classes are discovered as top-level classes and don't.
  private static Set<String> effectiveTags(Class<?> c) {
    Set<String> tags = new HashSet<>();
    for (Class<?> k = c;
        k != null;
        k = Modifier.isStatic(k.getModifiers()) ? null : k.getEnclosingClass()) {
      AnnotationSupport.findRepeatableAnnotations(k, Tag.class).forEach(t -> tags.add(t.value()));
    }
    return tags;
  }

  private static URL codeLocation(Class<?> c) {
    return c.getProtectionDomain().getCodeSource() == null
        ? null
        : c.getProtectionDomain().getCodeSource().getLocation();
  }
}
