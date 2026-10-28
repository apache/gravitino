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

import java.util.Objects;
import org.apache.gravitino.Namespace;

/** An immutable, decoded metric discovery result. */
public final class ImmutableSemanticModelMetric implements SemanticModelMetric {
  private final Namespace namespace;
  private final Metric metric;

  /**
   * Creates a metric discovery result.
   *
   * @param namespace The namespace ending with the containing model's name.
   * @param metric The complete metric definition.
   */
  public ImmutableSemanticModelMetric(Namespace namespace, Metric metric) {
    this.namespace = Objects.requireNonNull(namespace, "namespace must not be null");
    this.metric = Objects.requireNonNull(metric, "metric must not be null");
  }

  @Override
  public Namespace namespace() {
    return namespace;
  }

  @Override
  public Metric metric() {
    return metric;
  }
}
