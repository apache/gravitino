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

import java.io.Closeable;
import java.io.IOException;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.SemanticQueryPlanException;

/**
 * A pluggable Semantic Model query planner.
 *
 * <p>Gravitino owns request validation, authorization, model resolution, and the response envelope.
 * A planner owns SQL generation, join and fan-out rules, and the dialect matrix. Implementations
 * are selected with {@code gravitino.semanticModel.queryPlanner} and loaded from the class named by
 * {@code gravitino.semanticModelQueryPlanner.<name>.class}.
 *
 * <p>Implementations are created once and may be called concurrently, so they must be thread-safe.
 * The server never executes the generated SQL.
 */
@Evolving
public interface SemanticQueryPlanner extends Closeable {

  /**
   * Returns the planner name configured by {@code gravitino.semanticModel.queryPlanner}.
   *
   * @return The non-blank planner name.
   */
  String name();

  /**
   * Compiles a semantic query against a Semantic Model definition.
   *
   * @param model The resolved Semantic Model whose current definition is used.
   * @param query The validated semantic query.
   * @param context The planning context.
   * @return The generated plan.
   * @throws SemanticQueryPlanException If the query cannot be compiled for the requested dialect.
   */
  SemanticQueryPlan plan(SemanticModel model, SemanticQuery query, PlanningContext context);

  @Override
  default void close() throws IOException {}
}
