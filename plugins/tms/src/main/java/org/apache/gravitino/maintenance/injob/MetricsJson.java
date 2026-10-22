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
package org.apache.gravitino.maintenance.injob;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nullable;
import org.apache.gravitino.json.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Parses numeric fields from TMS in-job metrics JSON payloads. */
final class MetricsJson {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsJson.class);

  private MetricsJson() {}

  /**
   * @param metricsJson JSON object, possibly null/blank
   * @return numeric field bindings (integral → {@link Long}, otherwise {@link Double}); empty on
   *     parse failure
   */
  static Map<String, Object> parseNumericValues(@Nullable String metricsJson) {
    Map<String, Object> metrics = new HashMap<>();
    if (metricsJson == null || metricsJson.trim().isEmpty()) {
      return metrics;
    }
    try {
      JsonNode root = JsonUtils.anyFieldMapper().readTree(metricsJson);
      Iterator<Entry<String, JsonNode>> fields = root.fields();
      while (fields.hasNext()) {
        Entry<String, JsonNode> field = fields.next();
        JsonNode value = field.getValue();
        if (value == null || value.isNull() || !value.isNumber()) {
          continue;
        }
        if (value.isIntegralNumber()) {
          metrics.put(field.getKey(), value.longValue());
        } else {
          metrics.put(field.getKey(), value.doubleValue());
        }
      }
      return metrics;
    } catch (IOException | RuntimeException e) {
      LOG.warn("Failed to parse metrics JSON: {}", e.toString());
      return metrics;
    }
  }
}
