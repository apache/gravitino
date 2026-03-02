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

package org.apache.gravitino.maintenance.optimizer.recommender.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;

public class StatisticsUtils {

  private StatisticsUtils() {}

  public static Map<String, Object> buildStatisticsContext(
      List<StatisticEntry<?>> tableStatistics) {
    Map<String, Object> context = new HashMap<>();
    if (tableStatistics == null) {
      return context;
    }
    for (StatisticEntry<?> statistic : tableStatistics) {
      if (statistic != null && statistic.name() != null) {
        context.put(statistic.name(), statistic.value().value());
      }
    }
    return context;
  }
}
