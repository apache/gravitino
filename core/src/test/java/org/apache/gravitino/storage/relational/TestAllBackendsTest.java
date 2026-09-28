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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Tags;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.support.AnnotationSupport;

/**
 * Pins the contract of {@link AllBackendsTest}: JUnit must expand it to exactly the three backend
 * lane tags that {@code core/build.gradle.kts} filters on, for the annotated class and for its
 * subclasses, using the same annotation lookup the Jupiter engine runs at discovery time.
 */
public class TestAllBackendsTest {

  private static final List<String> BACKEND_TAGS =
      List.of(
          "gravitino-core-h2-test", "gravitino-core-mysql-test", "gravitino-core-postgresql-test");

  @Test
  void testExpandsToEveryBackendTag() {
    assertEquals(BACKEND_TAGS, tagsOf(Annotated.class));
  }

  @Test
  void testSubclassInheritsEveryBackendTag() {
    assertEquals(BACKEND_TAGS, tagsOf(Subclass.class));
  }

  @Test
  void testCarriesNothingButTags() {
    Set<Class<? extends Annotation>> metaAnnotations =
        Arrays.stream(AllBackendsTest.class.getAnnotations())
            .map(Annotation::annotationType)
            .collect(Collectors.toSet());

    // No @ExtendWith or other execution-time hook: lane filtering stays a discovery-time tag
    // match, so classes left out of a lane never appear in that lane's JUnit XML.
    assertEquals(
        Set.of(Documented.class, Inherited.class, Retention.class, Target.class, Tags.class),
        metaAnnotations);
  }

  private static List<String> tagsOf(Class<?> testClass) {
    return AnnotationSupport.findRepeatableAnnotations(testClass, Tag.class).stream()
        .map(Tag::value)
        .collect(Collectors.toList());
  }

  @AllBackendsTest
  private static class Annotated {}

  private static class Subclass extends Annotated {}
}
