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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Tag;
import org.junit.platform.commons.support.AnnotationSupport;

/**
 * A local, discovery-only check: prints which core database test lane(s) a class runs in, from its
 * tags, without running anything. Reads a class's tags with the same lookup the Jupiter engine uses
 * at discovery time (see {@link CoreBackend}), so the answer matches what {@code
 * core/build.gradle.kts}'s lane tasks would actually do.
 *
 * <p>Run with {@code ./gradlew :core:coreTestLaneOf -PclassName=<fully.qualified.ClassName>}.
 */
public final class CoreTestLaneOf {

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

    Set<String> tags =
        AnnotationSupport.findRepeatableAnnotations(testClass, Tag.class).stream()
            .map(Tag::value)
            .collect(Collectors.toSet());

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

    System.out.println(args[0] + " tags: " + tags);
    if (!lanes.isEmpty()) {
      System.out.println(args[0] + " runs in: " + String.join(", ", lanes));
      return;
    }

    if (tags.contains("gravitino-docker-test")) {
      System.out.println(
          args[0]
              + " carries gravitino-docker-test but no backend tag - it will NOT run in ANY"
              + " lane. Add @CoreBackend.H2/@CoreBackend.MySQL/@CoreBackend.PostgreSQL or"
              + " @CoreBackend.All.");
    } else {
      System.out.println(args[0] + " carries no backend tag - runs in coreUnitTest.");
    }
  }
}
