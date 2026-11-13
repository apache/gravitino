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
package org.apache.gravitino.listener.api.event.domain;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.ListEvent;
import org.apache.gravitino.listener.api.event.OperationType;

/** The success event for ListDomainsInfo. */
@DeveloperApi
public final class ListDomainsInfoEvent extends DomainEvent implements ListEvent {
  private final int resultCount;

  /**
   * Creates the event.
   *
   * @param user The initiating user.
   * @param metalake The metalake name.
   * @param resultCount The number of dispatcher results before REST authorization filtering, or -1
   *     if unavailable.
   */
  public ListDomainsInfoEvent(String user, String metalake, int resultCount) {
    super(user, NameIdentifier.of(metalake));
    this.resultCount = resultCount;
  }

  /**
   * @return The number of dispatcher results before REST authorization filtering, or -1 if
   *     unavailable.
   */
  @Override
  public int resultCount() {
    return resultCount;
  }

  /** {@inheritDoc} */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_DOMAINS_INFO;
  }
}
