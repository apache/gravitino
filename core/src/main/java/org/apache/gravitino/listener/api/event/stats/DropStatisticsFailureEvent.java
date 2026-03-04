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

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;

/** Event fired when dropping metadata object statistics fails. */
@DeveloperApi
public class DropStatisticsFailureEvent extends StatisticsFailureEvent {
  private final List<String> statisticNames;

  /**
   * Constructor for DropStatisticsFailureEvent.
   *
   * @param user the name of the user who initiated the drop statistics operation
   * @param identifier the identifier of the metadata object whose statistics were to be dropped
   * @param exception the exception that was thrown during the drop statistics operation
   * @param statisticNames the list of statistic names that were attempted to be dropped
   */
  public DropStatisticsFailureEvent(
      String user, NameIdentifier identifier, Exception exception, List<String> statisticNames) {
    super(user, identifier, exception);
    this.statisticNames = statisticNames;
  }

  /**
   * Gets the names of the statistics that were attempted to be dropped.
   *
   * @return list of statistic names
   */
  public List<String> statisticNames() {
    return statisticNames;
  }

  @Override
  public OperationType operationType() {
    return OperationType.DROP_STATISTICS;
  }
}
