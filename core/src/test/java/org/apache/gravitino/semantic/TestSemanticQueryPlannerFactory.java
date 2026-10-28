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
package org.apache.gravitino.semantic;

import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests {@link SemanticQueryPlannerFactory} configuration handling. */
public class TestSemanticQueryPlannerFactory {

  private static final String PLANNER_CONFIG = "gravitino.semanticModelQueryPlanner.custom.class";

  private static Config config() {
    return new Config(false) {};
  }

  @Test
  void testDefaultPlannerReportsPlanningUnsupported() {
    SemanticQueryPlanner planner = SemanticQueryPlannerFactory.create(config());

    Assertions.assertEquals(SemanticQueryPlannerFactory.NONE_PLANNER_NAME, planner.name());
    SemanticQuery query =
        SemanticQuery.builder()
            .withMetrics(Collections.singletonList("m"))
            .withDimensions(Collections.emptyList())
            .withFilters(Collections.emptyList())
            .withDialect("trino")
            .build();
    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                planner.plan(
                    null, query, PlanningContext.of(NameIdentifier.of("m", "s", "model"))));
    Assertions.assertTrue(exception.getMessage().contains("gravitino.semanticModel.queryPlanner"));
    Assertions.assertDoesNotThrow(planner::close);
  }

  @Test
  void testCustomPlannerIsLoadedByConfiguredName() {
    Config config = config();
    config.set(Configs.SEMANTIC_MODEL_QUERY_PLANNER, "custom");
    config.loadFromMap(Map.of(PLANNER_CONFIG, TestPlanner.class.getName()), key -> true);

    SemanticQueryPlanner planner = SemanticQueryPlannerFactory.create(config);

    Assertions.assertInstanceOf(TestPlanner.class, planner);
    Assertions.assertEquals("custom", planner.name());
    SemanticQueryPlan plan =
        planner.plan(
            null,
            SemanticQuery.builder()
                .withMetrics(Collections.singletonList("m"))
                .withDimensions(Collections.emptyList())
                .withFilters(Collections.emptyList())
                .withDialect("trino")
                .build(),
            PlanningContext.of(NameIdentifier.of("m", "s", "model")));
    Assertions.assertEquals("SELECT 1", plan.sql());
    Assertions.assertEquals("trino", plan.dialect());
  }

  @Test
  void testMissingPlannerClassConfigIsRejected() {
    Config config = config();
    config.set(Configs.SEMANTIC_MODEL_QUERY_PLANNER, "custom");

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> SemanticQueryPlannerFactory.create(config));
    Assertions.assertTrue(exception.getMessage().contains(PLANNER_CONFIG));
  }

  @Test
  void testUnloadablePlannerClassIsRejected() {
    Config config = config();
    config.set(Configs.SEMANTIC_MODEL_QUERY_PLANNER, "custom");
    config.loadFromMap(
        Map.of(PLANNER_CONFIG, "org.apache.gravitino.semantic.DoesNotExist"), key -> true);

    RuntimeException exception =
        Assertions.assertThrows(
            RuntimeException.class, () -> SemanticQueryPlannerFactory.create(config));
    Assertions.assertTrue(exception.getMessage().contains("can't be loaded"));
  }

  @Test
  void testPlannerMustImplementPlannerInterface() {
    Config config = config();
    config.set(Configs.SEMANTIC_MODEL_QUERY_PLANNER, "custom");
    config.loadFromMap(Map.of(PLANNER_CONFIG, String.class.getName()), key -> true);

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> SemanticQueryPlannerFactory.create(config));
    Assertions.assertTrue(exception.getMessage().contains(SemanticQueryPlanner.class.getName()));
  }

  /** A test planner instantiated reflectively by the factory. */
  public static class TestPlanner implements SemanticQueryPlanner {

    /** Creates the test planner. */
    public TestPlanner() {}

    @Override
    public String name() {
      return "custom";
    }

    @Override
    public SemanticQueryPlan plan(
        SemanticModel model, SemanticQuery query, PlanningContext context) {
      return SemanticQueryPlan.of("SELECT 1", query.dialect(), name(), Collections.emptyList());
    }
  }
}
