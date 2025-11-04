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

/** Event fired after dropping statistics on a metadata object. */
@DeveloperApi
public class DropStatisticsEvent extends StatisticsEvent {

  private final List<String> statisticNames;
  /**
   * Constructor for DropStatisticsEvent.
   *
   * @param user the user performing the operation
   * @param identifier the identifier of the metadata object
   * @param statisticNames the names of the statistics that were dropped
   */
  public DropStatisticsEvent(String user, NameIdentifier identifier, List<String> statisticNames) {
    super(user, identifier);
    this.statisticNames = statisticNames;
  }

  /**
   * Gets the names of the statistics that were dropped.
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
