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
import org.apache.gravitino.listener.api.event.ListEvent;
import org.apache.gravitino.listener.api.event.OperationType;

/**
 * Successful list-Semantic-Models event. Like {@link
 * org.apache.gravitino.listener.api.event.view.ListViewEvent}, listed identifiers are not stored on
 * the event.
 */
@DeveloperApi
public final class ListSemanticModelEvent extends SemanticModelEvent implements ListEvent {
  private final Namespace namespace;
  private final int semanticModelCount;

  /**
   * Constructs an instance of {@code ListSemanticModelEvent}.
   *
   * @param user The username of the individual who initiated the Semantic Model listing.
   * @param namespace The namespace from which Semantic Models were listed.
   * @param semanticModelCount The number of Semantic Models returned by the list operation.
   */
  public ListSemanticModelEvent(String user, Namespace namespace, int semanticModelCount) {
    super(user, NameIdentifier.of(namespace.levels()));
    this.namespace = namespace;
    this.semanticModelCount = semanticModelCount;
  }

  /**
   * Returns the namespace associated with this event.
   *
   * @return The namespace.
   */
  public Namespace namespace() {
    return namespace;
  }

  /** {@inheritDoc} */
  @Override
  public int resultCount() {
    return semanticModelCount;
  }

  @Override
  public OperationType operationType() {
    return OperationType.LIST_SEMANTIC_MODEL;
  }
}
