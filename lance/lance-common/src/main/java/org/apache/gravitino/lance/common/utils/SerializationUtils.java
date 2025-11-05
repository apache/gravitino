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
 *
 */
package org.apache.gravitino.lance.common.utils;

import com.google.common.collect.ImmutableMap;
import com.lancedb.lance.namespace.util.JsonUtil;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class SerializationUtils {

  private SerializationUtils() {
    // Utility class
  }

  // Lance REST uses a unique way to serialize and serialize table, please see:
  // see https://github.com/lancedb/lance-namespace/blob/2033b2fca126e87e56ba0d5ec19c5ec010c7a98f/
  // java/lance-namespace-core/src/main/java/com/lancedb/lance/namespace/rest/RestNamespace.java#L207-L208
  public static Map<String, String> deserializeProperties(String serializedProperties) {
    return StringUtils.isBlank(serializedProperties)
        ? ImmutableMap.of()
        : JsonUtil.parse(
            serializedProperties,
            jsonNode -> {
              Map<String, String> map = new HashMap<>();
              jsonNode
                  .fields()
                  .forEachRemaining(
                      entry -> {
                        map.put(entry.getKey(), entry.getValue().asText());
                      });
              return map;
            });
  }
}
