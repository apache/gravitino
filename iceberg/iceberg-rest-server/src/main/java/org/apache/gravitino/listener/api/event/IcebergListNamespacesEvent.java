/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Represent an event after listing Iceberg namespace successfully.
 *
 * <p>Only the result count is stored rather than the full list of namespaces to avoid excessive
 * memory usage in environments with large namespace listings.
 */
@DeveloperApi
public class IcebergListNamespacesEvent extends IcebergNamespaceEvent implements ListEvent {

  private final int namespaceCount;

  /**
   * Constructs an instance of {@code IcebergListNamespacesEvent}.
   *
   * @param icebergRequestContext the request context.
   * @param nameIdentifier the parent namespace identifier.
   * @param namespaceCount the number of namespaces returned by the list operation.
   */
  public IcebergListNamespacesEvent(
      IcebergRequestContext icebergRequestContext,
      NameIdentifier nameIdentifier,
      int namespaceCount) {
    super(icebergRequestContext, nameIdentifier);
    this.namespaceCount = namespaceCount;
  }

  /**
   * Constructs an instance of {@code IcebergListNamespacesEvent} without a count.
   *
   * @param icebergRequestContext the request context.
   * @param nameIdentifier the parent namespace identifier.
   * @deprecated Use {@link #IcebergListNamespacesEvent(IcebergRequestContext, NameIdentifier, int)}
   *     instead.
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public IcebergListNamespacesEvent(
      IcebergRequestContext icebergRequestContext, NameIdentifier nameIdentifier) {
    this(icebergRequestContext, nameIdentifier, -1);
  }

  /** {@inheritDoc} */
  @Override
  public int resultCount() {
    return namespaceCount;
  }

  @Override
  public OperationType operationType() {
    return OperationType.LIST_SCHEMA;
  }
}
