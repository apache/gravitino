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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates the Semantic Model query planner selected by the server configuration. */
public final class SemanticQueryPlannerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(SemanticQueryPlannerFactory.class);

  /** The name of the built-in planner that reports planning as unsupported. */
  public static final String NONE_PLANNER_NAME = "none";

  private static final String PLANNER_CONF_PREFIX = "gravitino.semanticModelQueryPlanner.";

  private static final String PLANNER_CLASS_SUFFIX = ".class";

  private SemanticQueryPlannerFactory() {}

  /**
   * Creates the configured Semantic Model query planner.
   *
   * <p>The built-in {@code none} planner is returned when no planner is configured. Otherwise the
   * class named by {@code gravitino.semanticModelQueryPlanner.<name>.class} is loaded and
   * instantiated through its no-argument constructor.
   *
   * @param config The server configuration.
   * @return The configured planner.
   * @throws IllegalArgumentException If a custom planner is selected but its class is not
   *     configured or does not implement {@link SemanticQueryPlanner}.
   * @throws RuntimeException If the configured class cannot be loaded or instantiated.
   */
  public static SemanticQueryPlanner create(Config config) {
    String plannerName = config.get(Configs.SEMANTIC_MODEL_QUERY_PLANNER);
    if (NONE_PLANNER_NAME.equals(plannerName)) {
      return new NoopSemanticQueryPlanner();
    }

    String plannerClassKey = PLANNER_CONF_PREFIX + plannerName + PLANNER_CLASS_SUFFIX;
    String className = StringUtils.trimToNull(config.getRawString(plannerClassKey));
    Preconditions.checkArgument(
        className != null,
        "Semantic Model query planner class must be specified by %s",
        plannerClassKey);

    Class<?> plannerClass = loadPlannerClass(plannerName, className);
    Preconditions.checkArgument(
        SemanticQueryPlanner.class.isAssignableFrom(plannerClass),
        "Semantic Model query planner %s must implement %s",
        className,
        SemanticQueryPlanner.class.getName());

    try {
      SemanticQueryPlanner planner =
          (SemanticQueryPlanner) plannerClass.getDeclaredConstructor().newInstance();
      LOG.info("Loaded Semantic Model query planner '{}' from class {}", plannerName, className);
      return planner;
    } catch (ReflectiveOperationException | LinkageError e) {
      throw new RuntimeException(
          String.format(
              "Failed to create Semantic Model query planner %s from class %s",
              plannerName, className),
          e);
    }
  }

  private static Class<?> loadPlannerClass(String plannerName, String className) {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException | LinkageError e) {
      throw new RuntimeException(
          String.format(
              "Failed to load Semantic Model query planner %s: class %s can't be loaded. Check %s",
              plannerName, className, PLANNER_CONF_PREFIX + plannerName + PLANNER_CLASS_SUFFIX),
          e);
    }
  }

  private static final class NoopSemanticQueryPlanner implements SemanticQueryPlanner {

    @Override
    public String name() {
      return NONE_PLANNER_NAME;
    }

    @Override
    public SemanticQueryPlan plan(
        SemanticModel model, SemanticQuery query, PlanningContext context) {
      throw new UnsupportedOperationException(
          "No Semantic Model query planner is configured; set gravitino.semanticModel.queryPlanner"
              + " to a planner name");
    }
  }
}
