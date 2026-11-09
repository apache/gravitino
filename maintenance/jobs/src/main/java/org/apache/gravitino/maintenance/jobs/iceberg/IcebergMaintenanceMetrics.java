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
package org.apache.gravitino.maintenance.jobs.iceberg;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Map;
import org.apache.gravitino.json.JsonUtils;

/** Metrics JSON helpers for TMS Iceberg maintenance actions. */
final class IcebergMaintenanceMetrics {

  private IcebergMaintenanceMetrics() {}

  static String toJson(Map<String, Object> metrics) {
    try {
      return JsonUtils.anyFieldMapper().writeValueAsString(metrics);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize metrics JSON", e);
    }
  }
}
