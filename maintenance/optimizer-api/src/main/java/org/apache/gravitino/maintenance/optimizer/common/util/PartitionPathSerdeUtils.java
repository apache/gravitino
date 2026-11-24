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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;

/** Shared serde helpers for converting between partition path and JSON string. */
public final class PartitionPathSerdeUtils {
  private PartitionPathSerdeUtils() {}

  /** Encodes partition path into JSON string, such as [{"p1":"v1"},{"p2":"v2"}]. */
  public static String encode(PartitionPath partitionPath) {
    Preconditions.checkArgument(partitionPath != null, "partitionPath must not be null");
    List<PartitionEntry> entries = partitionPath.entries();
    Preconditions.checkArgument(entries != null && !entries.isEmpty(), "partitionPath is empty");

    List<Map<String, String>> encoded = new ArrayList<>(entries.size());
    for (PartitionEntry entry : entries) {
      String name = entry.partitionName();
      String value = entry.partitionValue();
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "partitionName cannot be blank");
      Preconditions.checkArgument(StringUtils.isNotBlank(value), "partitionValue cannot be blank");
      Map<String, String> item = new LinkedHashMap<>(1);
      item.put(name, value);
      encoded.add(item);
    }

    try {
      return JsonUtils.objectMapper().writeValueAsString(encoded);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to encode partition path", e);
    }
  }

  /** Decodes partition path from JSON string, such as [{"p1":"v1"},{"p2":"v2"}]. */
  public static PartitionPath decode(String encodedPartitionPath) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(encodedPartitionPath), "encodedPartitionPath must not be blank");
    JsonNode decoded;
    try {
      decoded = JsonUtils.objectMapper().readTree(encodedPartitionPath);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to decode partition path", e);
    }
    Preconditions.checkArgument(
        decoded != null && decoded.isArray() && !decoded.isEmpty(), "partitionPath is empty");

    List<PartitionEntry> entries = new ArrayList<>(decoded.size());
    for (JsonNode item : decoded) {
      Preconditions.checkArgument(
          item != null && item.isObject() && item.size() == 1,
          "partition entry must contain one key/value pair");
      Map.Entry<String, JsonNode> kv = item.properties().iterator().next();
      String name = kv.getKey();
      JsonNode valueNode = kv.getValue();
      String value = valueNode == null || valueNode.isNull() ? null : valueNode.asText();
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "partitionName cannot be blank");
      Preconditions.checkArgument(StringUtils.isNotBlank(value), "partitionValue cannot be blank");
      entries.add(new PartitionEntryImpl(name, value));
    }

    return PartitionPath.of(entries);
  }
}
