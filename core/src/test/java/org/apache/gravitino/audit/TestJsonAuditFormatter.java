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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import java.time.OffsetDateTime;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.audit.v2.SimpleFormatterV2;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.listener.api.event.ListEvent;
import org.apache.gravitino.listener.api.event.ListMetalakeEvent;
import org.apache.gravitino.listener.api.event.OperationStatus;
import org.apache.gravitino.listener.api.event.OperationType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJsonAuditFormatter {

  private final JsonAuditFormatter formatter = new JsonAuditFormatter();

  @Test
  public void testFormatSerializesAllCoreFields() throws Exception {
    JsonNode node = toJsonNode(formatter.format(new StubEvent()));

    Assertions.assertEquals("test-user", node.get("user").asText());
    Assertions.assertEquals("LIST_TABLE", node.get("operation").asText());
    Assertions.assertEquals("LIST_TABLE", node.get("operationType").asText());
    Assertions.assertEquals("metalake.catalog", node.get("identifier").asText());
    Assertions.assertEquals("SUCCESS", node.get("status").asText());
    Assertions.assertEquals("SUCCESS", node.get("operationStatus").asText());
    Assertions.assertEquals("GRAVITINO_SERVER", node.get("eventSource").asText());
    Assertions.assertEquals("unknown", node.get("remoteAddress").asText());
    Assertions.assertTrue(node.has("customInfo"));
    Assertions.assertTrue(node.get("customInfo").isObject());
    Assertions.assertEquals(0, node.get("customInfo").size());

    OffsetDateTime timestamp = OffsetDateTime.parse(node.get("timestamp").asText());
    Assertions.assertEquals(0, timestamp.getNano() % 1_000_000);
  }

  @Test
  public void testSensitiveCustomInfoIsMasked() throws Exception {
    JsonNode node = toJsonNode(formatter.format(new StubEventWithSensitiveCustomInfo()));
    JsonNode customInfo = node.get("customInfo");

    Assertions.assertEquals("***", customInfo.get("Authorization").asText());
    Assertions.assertEquals("***", customInfo.get("cookie").asText());
    Assertions.assertEquals("***", customInfo.get("X-Amz-Security-Token").asText());
    Assertions.assertEquals("***", customInfo.get("s3.access-key-id").asText());
    Assertions.assertEquals("***", customInfo.get("jdbc-password").asText());
    Assertions.assertEquals("visible", customInfo.get("env").asText());
  }

  @Test
  public void testSimpleFormatterV2MasksSensitiveCustomInfo() {
    String formatted =
        new SimpleFormatterV2().format(new StubEventWithSensitiveCustomInfo()).toString();

    Assertions.assertTrue(formatted.contains("Authorization=***"));
    Assertions.assertTrue(formatted.contains("cookie=***"));
    Assertions.assertTrue(formatted.contains("X-Amz-Security-Token=***"));
    Assertions.assertTrue(formatted.contains("s3.access-key-id=***"));
    Assertions.assertTrue(formatted.contains("jdbc-password=***"));
    Assertions.assertTrue(formatted.contains("env=visible"));
    Assertions.assertFalse(formatted.contains("Bearer token"));
    Assertions.assertFalse(formatted.contains("session-token"));
    Assertions.assertFalse(formatted.contains("secret"));
  }

  @Test
  public void testListEventAddsResultCount() throws Exception {
    StubListEvent event = new StubListEvent();

    JsonNode node = toJsonNode(formatter.format(event));
    Assertions.assertEquals("LIST_TABLE", node.get("operationType").asText());
    Assertions.assertEquals(4, node.get("resultCount").asInt());
    Assertions.assertEquals("prod", node.get("customInfo").get("env").asText());
  }

  @Test
  public void testNullIdentifierSerializedAsNull() throws Exception {
    JsonNode node = toJsonNode(formatter.format(new ListMetalakeEvent("bob", 2)));

    Assertions.assertTrue(node.has("identifier"));
    Assertions.assertTrue(node.get("identifier").isNull());
    Assertions.assertEquals(2, node.get("resultCount").asInt());
  }

  private JsonNode toJsonNode(AuditLog auditLog) throws Exception {
    return JsonUtils.objectMapper().readTree(auditLog.toString());
  }

  static class StubEvent extends Event {
    StubEvent() {
      this("test-user", NameIdentifier.of("metalake", "catalog"));
    }

    StubEvent(String user, NameIdentifier identifier) {
      super(user, identifier);
    }

    @Override
    public OperationType operationType() {
      return OperationType.LIST_TABLE;
    }

    @Override
    public OperationStatus operationStatus() {
      return OperationStatus.SUCCESS;
    }

    @Override
    public EventSource eventSource() {
      return EventSource.GRAVITINO_SERVER;
    }
  }

  static class StubEventWithSensitiveCustomInfo extends StubEvent {
    @Override
    public Map<String, String> customInfo() {
      return ImmutableMap.<String, String>builder()
          .put("Authorization", "Bearer token")
          .put("cookie", "a=b")
          .put("X-Amz-Security-Token", "session-token")
          .put("s3.access-key-id", "ak")
          .put("jdbc-password", "secret")
          .put("env", "visible")
          .build();
    }
  }

  static class StubListEvent extends StubEvent implements ListEvent {
    StubListEvent() {
      super("alice", NameIdentifier.of("m", "c", "s"));
    }

    @Override
    public int resultCount() {
      return 4;
    }

    @Override
    public Map<String, String> customInfo() {
      return ImmutableMap.of("env", "prod");
    }
  }
}
