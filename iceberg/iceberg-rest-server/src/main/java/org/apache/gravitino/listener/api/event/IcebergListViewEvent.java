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
 * Represent an event after listing Iceberg view successfully.
 *
 * <p>Only the result count is stored rather than the full list of identifiers to avoid excessive
 * memory usage in environments with large view listings.
 */
@DeveloperApi
public class IcebergListViewEvent extends IcebergViewEvent implements ListEvent {

  private final int viewCount;

  /**
   * Constructs an instance of {@code IcebergListViewEvent}.
   *
   * @param icebergRequestContext the request context.
   * @param viewIdentifier the namespace identifier from which views were listed.
   * @param viewCount the number of views returned by the list operation.
   */
  public IcebergListViewEvent(
      IcebergRequestContext icebergRequestContext, NameIdentifier viewIdentifier, int viewCount) {
    super(icebergRequestContext, viewIdentifier);
    this.viewCount = viewCount;
  }

  /**
   * Constructs an instance of {@code IcebergListViewEvent} without a count.
   *
   * @param icebergRequestContext the request context.
   * @param viewIdentifier the namespace identifier from which views were listed.
   * @deprecated Use {@link #IcebergListViewEvent(IcebergRequestContext, NameIdentifier, int)}
   *     instead.
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public IcebergListViewEvent(
      IcebergRequestContext icebergRequestContext, NameIdentifier viewIdentifier) {
    this(icebergRequestContext, viewIdentifier, -1);
  }

  /** {@inheritDoc} */
  @Override
  public int resultCount() {
    return viewCount;
  }

  @Override
  public OperationType operationType() {
    return OperationType.LIST_VIEW;
  }
}
