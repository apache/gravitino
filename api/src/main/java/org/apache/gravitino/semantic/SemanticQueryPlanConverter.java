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

import java.util.Set;
import org.apache.gravitino.annotation.Evolving;

/**
 * Materializes a {@link SemanticQueryPlan} into an engine's own plan representation.
 *
 * <p>This is the only place where an engine dialect appears. Gravitino never renders SQL or selects
 * a dialect; a converter lives in the engine adapter, not in the server runtime. Implementations
 * must find at least one expression dialect they understand for every field in the plan.
 *
 * @param <T> The engine-specific plan representation.
 */
@Evolving
public interface SemanticQueryPlanConverter<T> {

  /**
   * Returns the engine or dialect family this converter targets.
   *
   * @return The engine identifier, for example {@code spark-sql}.
   */
  String engine();

  /**
   * Returns the Semantic Model expression dialects this converter can read.
   *
   * @return The supported dialect identifiers.
   */
  Set<String> expressionDialects();

  /**
   * Translates a plan into the engine's own plan representation.
   *
   * @param plan The dialect-neutral plan.
   * @param context Engine-side configuration that the plan must not know about.
   * @return The engine-specific plan.
   * @throws UnsupportedOperationException If the plan cannot be represented by this engine.
   */
  T materialize(SemanticQueryPlan plan, ConversionContext context);
}
