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

package org.apache.gravitino.maintenance.optimizer.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.stats.StatisticValue;

/** Shared serde helpers for {@link StatisticValue}. */
public final class StatisticValueSerdeUtils {

  private StatisticValueSerdeUtils() {}

  public static String toString(StatisticValue<?> value) {
    Preconditions.checkArgument(value != null, "StatisticValue cannot be null");
    try {
      return JsonUtils.anyFieldMapper().writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize StatisticValue: " + value, e);
    }
  }

  public static StatisticValue<?> fromString(String valueStr) {
    Preconditions.checkArgument(valueStr != null, "StatisticValue string cannot be null");
    try {
      return JsonUtils.anyFieldMapper().readValue(valueStr, StatisticValue.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "Failed to deserialize StatisticValue from: " + valueStr, e);
    }
  }
}
