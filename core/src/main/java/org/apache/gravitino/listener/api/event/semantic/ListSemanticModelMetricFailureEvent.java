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

package org.apache.gravitino.listener.api.event.semantic;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;

/** Failure event for listing Semantic Model metrics. */
@DeveloperApi
public final class ListSemanticModelMetricFailureEvent extends SemanticModelFailureEvent {
  private final Namespace namespace;

  /**
   * Constructs an instance of {@code ListSemanticModelMetricFailureEvent}.
   *
   * @param user The username of the individual who initiated the metric listing.
   * @param namespace The namespace from which Semantic Model metrics were listed.
   * @param exception The exception that caused the failure.
   */
  public ListSemanticModelMetricFailureEvent(
      String user, Namespace namespace, Exception exception) {
    super(user, NameIdentifier.of(namespace.levels()), exception);
    this.namespace = namespace;
  }

  /**
   * Returns the namespace associated with this event.
   *
   * @return The namespace.
   */
  public Namespace namespace() {
    return namespace;
  }

  @Override
  public OperationType operationType() {
    return OperationType.LIST_SEMANTIC_MODEL_METRIC;
  }
}
