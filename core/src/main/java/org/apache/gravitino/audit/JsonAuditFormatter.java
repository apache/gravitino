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

package org.apache.gravitino.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.gravitino.audit.v2.SimpleAuditLogV2;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.ListEvent;

/** Formatter that serializes audit logs as one JSON object per line. */
public class JsonAuditFormatter implements Formatter {

  @Override
  public AuditLog format(Event event) {
    return new JsonAuditLog(event);
  }
}

final class JsonAuditLog extends SimpleAuditLogV2 {

  private static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder().build();

  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneId.systemDefault());

  private final Event event;

  JsonAuditLog(Event event) {
    super(event);
    this.event = event;
  }

  @Override
  public String toString() {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("timestamp", TIMESTAMP_FORMATTER.format(Instant.ofEpochMilli(timestamp())));
    payload.put("user", user());
    payload.put("operation", operation());
    payload.put("operationType", operationType());
    payload.put("identifier", identifier());
    payload.put("status", status());
    payload.put("operationStatus", operationStatus());
    payload.put("eventSource", eventSource());
    payload.put("remoteAddress", remoteAddress());
    payload.put("customInfo", AuditLogRedactor.redactCustomInfo(customInfo()));

    if (event instanceof ListEvent) {
      int resultCount = ((ListEvent) event).resultCount();
      if (resultCount >= 0) {
        payload.put("resultCount", resultCount);
      }
    }

    try {
      return OBJECT_MAPPER.writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize audit log to JSON", e);
    }
  }
}
