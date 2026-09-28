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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.annotation.Annotation;
import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Tags;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.platform.commons.support.AnnotationSupport;

/**
 * Pins the contract of {@link CoreBackend.H2}, {@link CoreBackend.MySQL}, {@link
 * CoreBackend.PostgreSQL}, and {@link CoreBackend.All}: each must expand to exactly the tag(s)
 * {@code core/build.gradle.kts} filters on, for the annotated class and for its subclasses, using
 * the same annotation lookup the Jupiter engine runs at discovery time - and none may carry an
 * execution-time hook, since that would repeat the mistake a prior, reverted design made.
 */
class TestCoreDatabaseLaneAnnotations {

  private static Stream<Object[]> annotationsAndExpectedTags() {
    return Stream.of(
        new Object[] {H2Annotated.class, List.of(CoreBackend.H2_TAG)},
        new Object[] {MySQLAnnotated.class, List.of(CoreBackend.MYSQL_TAG)},
        new Object[] {PostgreSQLAnnotated.class, List.of(CoreBackend.POSTGRESQL_TAG)},
        new Object[] {
          AllAnnotated.class,
          List.of(CoreBackend.H2_TAG, CoreBackend.MYSQL_TAG, CoreBackend.POSTGRESQL_TAG)
        });
  }

  // Default JUnit display names include argument toStrings - for expectedTags that would print a
  // tag string like "[gravitino-core-h2-test]" into this class's own JUnit XML, which is itself
  // part of the coreUnitTest lane, and core_test_identity.py's manifest step rejects any standalone
  // backend token there as a foreign-lane marker. Naming on {0} (the class under test) only avoids
  // that: its simple name (e.g. H2Annotated) has no such token, since "h2"/"mysql"/"postgresql" is
  // never followed by a non-letter there.
  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("annotationsAndExpectedTags")
  void testExpandsToExpectedTags(Class<?> annotatedClass, List<String> expectedTags) {
    assertEquals(expectedTags, tagsOf(annotatedClass));
  }

  @Test
  void testStackingTwoAnnotationsRunsUnderBothLanes() {
    assertEquals(List.of(CoreBackend.H2_TAG, CoreBackend.MYSQL_TAG), tagsOf(H2AndMySQL.class));
  }

  @Test
  void testSubclassInheritsTags() {
    assertEquals(List.of(CoreBackend.H2_TAG), tagsOf(H2Subclass.class));
    assertEquals(
        List.of(CoreBackend.H2_TAG, CoreBackend.MYSQL_TAG, CoreBackend.POSTGRESQL_TAG),
        tagsOf(AllSubclass.class));
  }

  @Test
  void testNoneOfThemCarryAnExecutionTimeHook() {
    // No @ExtendWith or similar on any of them: lane filtering stays a discovery-time tag
    // match, so classes left out of a lane never appear in that lane's JUnit XML.
    Set<Class<? extends Annotation>> tagOnlyMetaAnnotations =
        Set.of(
            Documented.class,
            Inherited.class,
            Retention.class,
            Target.class,
            Tag.class,
            Tags.class);
    for (Class<?> annotationType :
        List.of(
            CoreBackend.H2.class,
            CoreBackend.MySQL.class,
            CoreBackend.PostgreSQL.class,
            CoreBackend.All.class)) {
      Set<Class<? extends Annotation>> metaAnnotations =
          Arrays.stream(annotationType.getAnnotations())
              .map(Annotation::annotationType)
              .collect(Collectors.toSet());
      assertEquals(
          Set.of(),
          metaAnnotations.stream()
              .filter(type -> !tagOnlyMetaAnnotations.contains(type))
              .collect(Collectors.toSet()),
          annotationType.getSimpleName() + " carries an unexpected, non-tag meta-annotation");
    }
  }

  private static List<String> tagsOf(Class<?> testClass) {
    return AnnotationSupport.findRepeatableAnnotations(testClass, Tag.class).stream()
        .map(Tag::value)
        .collect(Collectors.toList());
  }

  @CoreBackend.H2
  private static class H2Annotated {}

  @CoreBackend.MySQL
  private static class MySQLAnnotated {}

  @CoreBackend.PostgreSQL
  private static class PostgreSQLAnnotated {}

  @CoreBackend.All
  private static class AllAnnotated {}

  @CoreBackend.H2
  @CoreBackend.MySQL
  private static class H2AndMySQL {}

  private static class H2Subclass extends H2Annotated {}

  private static class AllSubclass extends AllAnnotated {}
}
