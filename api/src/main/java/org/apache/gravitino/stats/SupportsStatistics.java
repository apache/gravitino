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
package org.apache.gravitino.stats;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.IllegalStatisticNameException;
import org.apache.gravitino.exceptions.UnmodifiableStatisticException;

/**
 * SupportsStatistics provides methods to list and update statistics. A table, a partition or a
 * fileset can implement this interface to manage its statistics.
 */
@Evolving
public interface SupportsStatistics {

  /**
   * Lists all statistics.
   *
   * @return a list of statistics
   */
  List<Statistic> listStatistics();

  /**
   * Updates statistics with the provided values. If the statistic exists, it will be updated with
   * the new value. If the statistic does not exist, it will be created. If the statistic is
   * unmodifiable, it will throw an UnmodifiableStatisticException. If the statistic name is
   * illegal, it will throw an IllegalStatisticNameException.
   *
   * @param statistics a map of statistic names to their values
   */
  void updateStatistics(Map<String, StatisticValue<?>> statistics)
      throws UnmodifiableStatisticException, IllegalStatisticNameException;

  /**
   * Drop statistics by their names. If the statistic is unmodifiable, it will throw an
   * UnmodifiableStatisticException.
   *
   * @param statistics a list of statistic names to be dropped
   * @return true if the statistics were successfully dropped, false if no statistics were dropped
   * @throws UnmodifiableStatisticException if any of the statistics to be dropped are unmodifiable
   */
  boolean dropStatistics(List<String> statistics) throws UnmodifiableStatisticException;
}
