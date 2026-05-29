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
package org.apache.gravitino.listener.api.event.stats;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.ListEvent;
import org.apache.gravitino.listener.api.event.OperationType;

/** Event fired when listing statistics. */
@DeveloperApi
public class ListStatisticsEvent extends StatisticsEvent implements ListEvent {

  private final int count;

  /**
   * Constructor for creating a list statistics event.
   *
   * @param user the user initiating the event
   * @param identifier the name identifier associated with the event.
   * @param count the number of statistics returned by the list operation.
   */
  public ListStatisticsEvent(String user, NameIdentifier identifier, int count) {
    super(user, identifier);
    this.count = count;
  }

  /**
   * Constructor for creating a list statistics event without a count.
   *
   * @param user the user initiating the event
   * @param identifier the name identifier associated with the event.
   * @deprecated Use {@link #ListStatisticsEvent(String, NameIdentifier, int)} instead.
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public ListStatisticsEvent(String user, NameIdentifier identifier) {
    this(user, identifier, -1);
  }

  /** {@inheritDoc} */
  @Override
  public int resultCount() {
    return count;
  }

  @Override
  public OperationType operationType() {
    return OperationType.LIST_STATISTICS;
  }
}
