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

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.platform.commons.annotation.Testable;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.HierarchyTraversalMode;
import org.junit.platform.commons.support.ReflectionSupport;

/**
 * A local, discovery-only check: prints which core database test lane(s) a class runs in, from its
 * tags, without running anything. Reads a class's tags with the same lookup the Jupiter engine uses
 * at discovery time (see {@link CoreBackend}), so the answer matches what {@code
 * core/build.gradle.kts}'s lane tasks would actually do. A class with no tests of its own, such as
 * {@code TestJdbcPartitionStatisticStorageIT}, is reported through its member test classes.
 *
 * <p>Run with {@code ./gradlew :core:coreTestLaneOf -PclassName=<fully.qualified.ClassName>}.
 */
public final class CoreTestLaneOf {

  static final String DOCKER_TAG = "gravitino-docker-test";

  private CoreTestLaneOf() {}

  /**
   * Entry point.
   *
   * @param args exactly one fully-qualified class name to inspect
   */
  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Usage: CoreTestLaneOf <fully.qualified.ClassName>");
      System.exit(1);
      return;
    }

    Class<?> testClass;
    try {
      testClass = Class.forName(args[0]);
    } catch (ClassNotFoundException e) {
      System.err.println("Class not found on the test classpath: " + args[0]);
      System.exit(1);
      return;
    }

    if (declaresTests(testClass)) {
      report(testClass);
      return;
    }

    List<Class<?>> members = new ArrayList<>();
    Arrays.stream(testClass.getDeclaredClasses())
        .filter(CoreTestLaneOf::declaresTests)
        .sorted(Comparator.comparing(Class::getName))
        .forEach(members::add);
    if (members.isEmpty()) {
      System.out.println(args[0] + " has no tests of its own or in its member classes.");
      return;
    }
    System.out.println(args[0] + " has no tests of its own; reporting its member test classes:");
    members.forEach(CoreTestLaneOf::report);
  }

  /**
   * Whether JUnit would run {@code c}: concrete, with at least one {@code @Test}-style method (all
   * meta-annotated with {@link Testable}), own or inherited.
   */
  static boolean declaresTests(Class<?> c) {
    return !Modifier.isAbstract(c.getModifiers())
        && !ReflectionSupport.findMethods(
                c,
                m -> AnnotationSupport.isAnnotated(m, Testable.class),
                HierarchyTraversalMode.TOP_DOWN)
            .isEmpty();
  }

  /**
   * Own and inherited tags, plus the enclosing classes' tags for non-static {@code @Nested}
   * classes, which JUnit applies to them. Static member classes are discovered as top-level classes
   * and don't inherit their enclosing class's tags.
   */
  static Set<String> effectiveTags(Class<?> c) {
    Set<String> tags = new HashSet<>();
    for (Class<?> k = c;
        k != null;
        k = Modifier.isStatic(k.getModifiers()) ? null : k.getEnclosingClass()) {
      AnnotationSupport.findRepeatableAnnotations(k, Tag.class).forEach(t -> tags.add(t.value()));
    }
    return tags;
  }

  private static void report(Class<?> c) {
    String name = c.getName();
    Set<String> tags = effectiveTags(c);
    List<String> lanes = new ArrayList<>();
    if (tags.contains(CoreBackend.H2_TAG)) {
      lanes.add("coreH2Test");
    }
    if (tags.contains(CoreBackend.MYSQL_TAG)) {
      lanes.add("coreMySQLTest");
    }
    if (tags.contains(CoreBackend.POSTGRESQL_TAG)) {
      lanes.add("corePostgreSQLTest");
    }

    System.out.println(name + " tags: " + tags);
    if (!lanes.isEmpty()) {
      System.out.println(name + " runs in: " + String.join(", ", lanes));
    } else if (tags.contains(DOCKER_TAG)) {
      System.out.println(
          name
              + " carries gravitino-docker-test but no backend tag - it will NOT run in ANY"
              + " lane. Add @CoreBackend.H2/@CoreBackend.MySQL/@CoreBackend.PostgreSQL or"
              + " @CoreBackend.All.");
    } else {
      System.out.println(name + " carries no backend tag - runs in coreUnitTest.");
    }
  }
}
