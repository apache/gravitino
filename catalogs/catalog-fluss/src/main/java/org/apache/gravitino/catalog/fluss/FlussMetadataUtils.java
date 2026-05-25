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

package org.apache.gravitino.catalog.fluss;

import static org.apache.gravitino.StringIdentifier.ID_KEY;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.gravitino.meta.AuditInfo;

final class FlussMetadataUtils {

  static Map<String, String> removeInternalProperties(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return new LinkedHashMap<>();
    }

    return properties.entrySet().stream()
        .filter(entry -> !ID_KEY.equals(entry.getKey()))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (left, right) -> right,
                LinkedHashMap::new));
  }

  static String requireTopLevelField(String[] fieldName) {
    if (fieldName.length != 1 || Objects.requireNonNull(fieldName[0]).isEmpty()) {
      throw new IllegalArgumentException("Fluss only supports top-level fields");
    }
    return fieldName[0];
  }

  static AuditInfo toAuditInfo(long createdTime, long modifiedTime) {
    return AuditInfo.builder()
        .withCreateTime(createdTime <= 0 ? null : Instant.ofEpochMilli(createdTime))
        .withLastModifiedTime(modifiedTime <= 0 ? null : Instant.ofEpochMilli(modifiedTime))
        .build();
  }

  private FlussMetadataUtils() {}
}
