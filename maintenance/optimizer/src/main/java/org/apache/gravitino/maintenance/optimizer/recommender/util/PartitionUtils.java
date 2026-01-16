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

import com.fasterxml.jackson.core.type.TypeReference;
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

/** Helpers for converting between Gravitino partition names and {@link PartitionPath}. */
public class PartitionUtils {
  private static final TypeReference<List<Map<String, String>>> PARTITION_PATH_TYPE =
      new TypeReference<List<Map<String, String>>>() {};

  private PartitionUtils() {}

  /**
   * Encodes a {@link PartitionPath} into a JSON string.
   *
   * <p>For example, a path with entries {@code p1=v1, p2=v2} is encoded as {@code [{"p1":"v1"},
   * {"p2":"v2"}]}.
   *
   * @param partitionPath partition path
   * @return encoded JSON string
   */
  public static String encodePartitionPath(PartitionPath partitionPath) {
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

  /**
   * Decodes a JSON-encoded partition path into a {@link PartitionPath}.
   *
   * <p>Example format: {@code [{"p1":"v1"},{"p2":"v2"}]}.
   *
   * @param encodedPartitionPath JSON string representing the partition path
   * @return parsed partition path
   */
  public static PartitionPath decodePartitionPath(String encodedPartitionPath) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(encodedPartitionPath), "encodedPartitionPath must not be blank");
    List<Map<String, String>> decoded;
    try {
      decoded = JsonUtils.objectMapper().readValue(encodedPartitionPath, PARTITION_PATH_TYPE);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to decode partition path", e);
    }
    Preconditions.checkArgument(decoded != null && !decoded.isEmpty(), "partitionPath is empty");

    List<PartitionEntry> entries = new ArrayList<>(decoded.size());
    for (Map<String, String> item : decoded) {
      Preconditions.checkArgument(
          item != null && item.size() == 1, "partition entry must contain one key/value pair");
      Map.Entry<String, String> kv = item.entrySet().iterator().next();
      String name = kv.getKey();
      String value = kv.getValue();
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "partitionName cannot be blank");
      Preconditions.checkArgument(StringUtils.isNotBlank(value), "partitionValue cannot be blank");
      entries.add(new PartitionEntryImpl(name, value));
    }

    return PartitionPath.of(entries);
  }
}
