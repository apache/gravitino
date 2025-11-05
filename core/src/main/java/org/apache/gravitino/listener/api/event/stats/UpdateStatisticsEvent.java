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

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.stats.StatisticValue;

/** Event fired when updating statistics. */
@DeveloperApi
public class UpdateStatisticsEvent extends StatisticsEvent {
  private final Map<String, StatisticValue<?>> statistics;

  /**
   * Constructor for UpdateStatisticsEvent.
   *
   * @param user the user performing the operation
   * @param identifier the identifier of the metadata object
   * @param statistics the statistics to be updated
   */
  public UpdateStatisticsEvent(
      String user, NameIdentifier identifier, Map<String, StatisticValue<?>> statistics) {
    super(user, identifier);
    this.statistics = statistics;
  }

  /**
   * Gets the statistics to be updated.
   *
   * @return map of statistic names to their values
   */
  public Map<String, StatisticValue<?>> statistics() {
    return statistics;
  }

  @Override
  public OperationType operationType() {
    return OperationType.UPDATE_STATISTICS;
  }
}
