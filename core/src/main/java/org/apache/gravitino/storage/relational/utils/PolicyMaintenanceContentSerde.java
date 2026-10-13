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
package org.apache.gravitino.storage.relational.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.gravitino.maintenance.policy.MaintenancePeerFields;
import org.apache.gravitino.maintenance.policy.MaintenanceTrigger;
import org.apache.gravitino.meta.PolicyEntity;

/** Merges and splits TMS peer fields in stored policy content JSON. */
final class PolicyMaintenanceContentSerde {

  private PolicyMaintenanceContentSerde() {}

  static String serializePolicyEntityContent(PolicyEntity policyEntity, ObjectMapper mapper)
      throws com.fasterxml.jackson.core.JsonProcessingException {
    ObjectNode node = mapper.valueToTree(policyEntity.content());
    MaintenancePeerFields peerFields = policyEntity.maintenancePeerFields();
    if (peerFields != null) {
      writePeerFields(node, peerFields, mapper);
    }
    return mapper.writeValueAsString(node);
  }

  static SplitResult splitFromContentJson(String contentJson, ObjectMapper mapper) {
    try {
      JsonNode root = mapper.readTree(contentJson);
      if (!(root instanceof ObjectNode)) {
        return new SplitResult(MaintenancePeerFields.empty(), root);
      }
      ObjectNode objectNode = root.deepCopy();
      MaintenancePeerFields peers = readAndRemovePeerFields(objectNode, mapper);
      return new SplitResult(peers, objectNode);
    } catch (Exception e) {
      throw new RuntimeException("Failed to split maintenance peer fields from policy JSON", e);
    }
  }

  private static MaintenancePeerFields readAndRemovePeerFields(
      ObjectNode root, ObjectMapper mapper) {
    MaintenanceTrigger schedule = null;
    if (root.has(MaintenancePeerFields.SCHEDULE_JSON_KEY)
        && !root.get(MaintenancePeerFields.SCHEDULE_JSON_KEY).isNull()) {
      schedule =
          mapper.convertValue(
              root.get(MaintenancePeerFields.SCHEDULE_JSON_KEY), MaintenanceTrigger.class);
      root.remove(MaintenancePeerFields.SCHEDULE_JSON_KEY);
    }

    Long minIntervalMs = null;
    if (root.has(MaintenancePeerFields.MIN_INTERVAL_MS_JSON_KEY)
        && !root.get(MaintenancePeerFields.MIN_INTERVAL_MS_JSON_KEY).isNull()) {
      minIntervalMs = root.get(MaintenancePeerFields.MIN_INTERVAL_MS_JSON_KEY).asLong();
      root.remove(MaintenancePeerFields.MIN_INTERVAL_MS_JSON_KEY);
    }

    Map<String, String> jobOptions =
        readStringMapAndRemove(root, MaintenancePeerFields.JOB_OPTIONS_JSON_KEY);
    Map<String, String> updateOptions =
        readStringMapAndRemove(root, MaintenancePeerFields.UPDATE_OPTIONS_JSON_KEY);

    return MaintenancePeerFields.of(schedule, minIntervalMs, jobOptions, updateOptions);
  }

  private static void writePeerFields(
      ObjectNode root, MaintenancePeerFields peerFields, ObjectMapper mapper) {
    if (!peerFields.hasAnyField()) {
      return;
    }
    if (peerFields.schedule() != null) {
      root.set(MaintenancePeerFields.SCHEDULE_JSON_KEY, mapper.valueToTree(peerFields.schedule()));
    }
    if (peerFields.minIntervalMs() != null) {
      root.put(MaintenancePeerFields.MIN_INTERVAL_MS_JSON_KEY, peerFields.minIntervalMs());
    }
    if (!peerFields.jobOptions().isEmpty()) {
      root.set(
          MaintenancePeerFields.JOB_OPTIONS_JSON_KEY, mapper.valueToTree(peerFields.jobOptions()));
    }
    if (!peerFields.updateOptions().isEmpty()) {
      root.set(
          MaintenancePeerFields.UPDATE_OPTIONS_JSON_KEY,
          mapper.valueToTree(peerFields.updateOptions()));
    }
  }

  private static Map<String, String> readStringMapAndRemove(ObjectNode root, String key) {
    if (!root.has(key) || root.get(key).isNull()) {
      return null;
    }
    JsonNode node = root.get(key);
    root.remove(key);
    if (!node.isObject()) {
      return null;
    }
    Map<String, String> map = new LinkedHashMap<>();
    node.fields().forEachRemaining(entry -> map.put(entry.getKey(), entry.getValue().asText()));
    return map.isEmpty() ? null : map;
  }

  static final class SplitResult {
    private final MaintenancePeerFields peerFields;
    private final JsonNode taskContent;

    private SplitResult(MaintenancePeerFields peerFields, JsonNode taskContent) {
      this.peerFields = peerFields;
      this.taskContent = taskContent;
    }

    MaintenancePeerFields peerFields() {
      return peerFields;
    }

    JsonNode taskContent() {
      return taskContent;
    }
  }
}
