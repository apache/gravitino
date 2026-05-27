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

package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.DeveloperApi;

/** Represents an event that is generated after a model listing operation. */
@DeveloperApi
public class ListModelEvent extends ModelEvent implements ListEvent {
  private final Namespace namespace;
  private final int modelCount;

  /**
   * Constructs an instance of {@code ListModelEvent}, with the specified user, namespace and count.
   *
   * @param user The username of the individual who initiated the model listing.
   * @param namespace The namespace from which models were listed.
   * @param modelCount The number of models returned by the list operation.
   */
  public ListModelEvent(String user, Namespace namespace, int modelCount) {
    super(user, NameIdentifier.of(namespace.levels()));
    this.namespace = namespace;
    this.modelCount = modelCount;
  }

  /**
   * Constructs an instance of {@code ListModelEvent} without a count.
   *
   * @param user The username of the individual who initiated the model listing.
   * @param namespace The namespace from which models were listed.
   * @deprecated Use {@link #ListModelEvent(String, Namespace, int)} instead.
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public ListModelEvent(String user, Namespace namespace) {
    this(user, namespace, -1);
  }

  /**
   * Provides the namespace associated with this event.
   *
   * @return A {@link Namespace} instance from which models were listed.
   */
  public Namespace namespace() {
    return namespace;
  }

  /** {@inheritDoc} */
  @Override
  public int resultCount() {
    return modelCount;
  }

  /**
   * Returns the type of operation.
   *
   * @return The operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_MODEL;
  }
}
