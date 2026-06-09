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

package org.apache.gravitino.listener.api.event.function;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.ListEvent;
import org.apache.gravitino.listener.api.event.OperationType;

/**
 * Represents an event that is triggered upon the successful listing of functions within a
 * namespace.
 */
@DeveloperApi
public final class ListFunctionEvent extends FunctionEvent implements ListEvent {
  private final Namespace namespace;
  private final int functionCount;

  /**
   * Constructs an instance of {@code ListFunctionEvent}.
   *
   * @param user The username of the individual who initiated the function listing.
   * @param namespace The namespace from which functions were listed.
   * @param functionCount The number of functions returned by the list operation.
   */
  public ListFunctionEvent(String user, Namespace namespace, int functionCount) {
    super(user, NameIdentifier.of(namespace.levels()));
    this.namespace = namespace;
    this.functionCount = functionCount;
  }

  /**
   * Constructs an instance of {@code ListFunctionEvent} without a count.
   *
   * @param user The username of the individual who initiated the function listing.
   * @param namespace The namespace from which functions were listed.
   * @deprecated Use {@link #ListFunctionEvent(String, Namespace, int)} instead.
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public ListFunctionEvent(String user, Namespace namespace) {
    this(user, namespace, -1);
  }

  /**
   * Provides the namespace associated with this event.
   *
   * @return A {@link Namespace} instance from which functions were listed.
   */
  public Namespace namespace() {
    return namespace;
  }

  /** {@inheritDoc} */
  @Override
  public int resultCount() {
    return functionCount;
  }

  @Override
  public OperationType operationType() {
    return OperationType.LIST_FUNCTION;
  }
}
