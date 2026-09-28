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

import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

/**
 * Restricts a {@link DatabaseTest} class to the backends it declares. Replaces the three {@code
 * excludeTags(...)} clauses {@code registerCoreTestTask} used to hand-maintain per lane: with this
 * condition, {@code core/build.gradle.kts} only needs {@code includeTags(coreDatabaseTestTag)} plus
 * the {@code gravitino.core.test.backend} system property each lane already publishes for {@link
 * BackendTestSelector}.
 */
public class BackendLaneCondition implements ExecutionCondition {

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    Optional<DatabaseTest> annotation =
        context
            .getTestClass()
            .flatMap(c -> AnnotationSupport.findAnnotation(c, DatabaseTest.class));
    if (!annotation.isPresent()) {
      return ConditionEvaluationResult.enabled("Not a @DatabaseTest class");
    }

    Optional<String> selectedBackend = BackendTestSelector.selectedBackend();
    if (!selectedBackend.isPresent()) {
      // No lane-specific backend was selected (e.g. the legacy :core:test invocation) - run
      // under every backend this class declares, same as before this condition existed.
      return ConditionEvaluationResult.enabled("No lane backend selected");
    }

    boolean applicable =
        Arrays.stream(annotation.get().backends())
            .anyMatch(backend -> backend.propertyValue().equals(selectedBackend.get()));

    return applicable
        ? ConditionEvaluationResult.enabled("Applies to backend " + selectedBackend.get())
        : ConditionEvaluationResult.disabled(
            "@DatabaseTest does not declare backend " + selectedBackend.get());
  }
}
