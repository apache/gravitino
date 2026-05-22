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

package org.apache.gravitino.audit.v2;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.listener.api.event.ListCatalogEvent;
import org.apache.gravitino.listener.api.event.ListMetalakeEvent;
import org.apache.gravitino.listener.api.event.ListSchemaEvent;
import org.apache.gravitino.listener.api.event.ListTableEvent;
import org.apache.gravitino.listener.api.event.OperationStatus;
import org.apache.gravitino.listener.api.event.OperationType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSimpleAuditLogV2 {

  @Test
  public void testTimestampHasMillisecondPrecision() {
    SimpleAuditLogV2 log = new SimpleAuditLogV2(new StubEvent());
    String output = log.toString();
    // Format is [yyyy-MM-dd HH:mm:ss.SSS] — the dot-separated millis must be present.
    Assertions.assertTrue(
        output.matches("\\[\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}\\].*"),
        "Expected millisecond precision in: " + output);
  }

  @Test
  public void testCustomInfoAppendedWhenPresent() {
    SimpleAuditLogV2 log = new SimpleAuditLogV2(new StubEventWithCustomInfo());
    String output = log.toString();
    String[] fields = output.split("\t", -1);
    // 8 tab-separated fields expected: timestamp, user, opType, id, status, source, addr, custom
    Assertions.assertEquals(8, fields.length, "Expected 8 tab-separated fields, got: " + output);
    Assertions.assertTrue(
        fields[7].contains("k1"), "Last field should contain customInfo key: " + fields[7]);
  }

  @Test
  public void testCustomInfoEmptyWhenAbsent() {
    SimpleAuditLogV2 log = new SimpleAuditLogV2(new StubEvent());
    String output = log.toString();
    String[] fields = output.split("\t", -1);
    Assertions.assertEquals(8, fields.length);
    Assertions.assertEquals("", fields[7], "Last field should be empty when customInfo is absent");
  }

  @Test
  public void testOutputContainsAllCoreFields() {
    SimpleAuditLogV2 log = new SimpleAuditLogV2(new StubEvent());
    String output = log.toString();
    Assertions.assertTrue(output.contains("test-user"));
    Assertions.assertTrue(output.contains("LIST_TABLE"));
    Assertions.assertTrue(output.contains("metalake.catalog"));
    Assertions.assertTrue(output.contains("SUCCESS"));
  }

  // ---- list event tests ----
  @Test
  public void testListTableEventFormat() {
    Namespace namespace = Namespace.of("metalake", "catalog", "schema");
    ListTableEvent event = new ListTableEvent("alice", namespace, 5);
    SimpleAuditLogV2 log = new SimpleAuditLogV2(event);

    Assertions.assertEquals(OperationType.LIST_TABLE, log.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, log.operationStatus());
    Assertions.assertEquals("metalake.catalog.schema", log.identifier());
    Assertions.assertEquals("alice", log.user());

    String output = log.toString();
    String[] fields = output.split("\t", -1);
    Assertions.assertEquals(8, fields.length);
    Assertions.assertTrue(
        fields[0].matches("\\[\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}\\]"));
    Assertions.assertEquals("alice", fields[1]);
    Assertions.assertEquals("LIST_TABLE", fields[2]);
    Assertions.assertEquals("metalake.catalog.schema", fields[3]);
    Assertions.assertEquals("SUCCESS", fields[4]);
    Assertions.assertEquals("GRAVITINO_SERVER", fields[5]);
    Assertions.assertEquals("unknown", fields[6]); // no RequestContext set in tests
    Assertions.assertEquals("{count=5}", fields[7]); // count surfaced in last field
  }

  @Test
  public void testListMetalakeEventNullIdentifier() {
    ListMetalakeEvent event = new ListMetalakeEvent("bob", 3);
    SimpleAuditLogV2 log = new SimpleAuditLogV2(event);

    Assertions.assertNull(log.identifier());
    Assertions.assertEquals(OperationType.LIST_METALAKE, log.operationType());

    // toString() must not throw and must have 8 fields with count in last field.
    String[] fields = log.toString().split("\t", -1);
    Assertions.assertEquals(8, fields.length);
    Assertions.assertEquals("bob", fields[1]);
    Assertions.assertEquals("LIST_METALAKE", fields[2]);
    Assertions.assertEquals("null", fields[3]);
    Assertions.assertEquals("{count=3}", fields[7]);
  }

  @Test
  public void testListCatalogEventFormat() {
    Namespace namespace = Namespace.of("metalake");
    ListCatalogEvent event = new ListCatalogEvent("carol", namespace, 2);
    SimpleAuditLogV2 log = new SimpleAuditLogV2(event);

    Assertions.assertEquals("metalake", log.identifier());
    Assertions.assertEquals(OperationType.LIST_CATALOG, log.operationType());

    String[] fields = log.toString().split("\t", -1);
    Assertions.assertEquals("LIST_CATALOG", fields[2]);
    Assertions.assertEquals("metalake", fields[3]);
    Assertions.assertEquals("{count=2}", fields[7]);
  }

  @Test
  public void testListSchemaEventFormat() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    ListSchemaEvent event = new ListSchemaEvent("dave", namespace, 7);
    SimpleAuditLogV2 log = new SimpleAuditLogV2(event);

    String[] fields = log.toString().split("\t", -1);
    Assertions.assertEquals("LIST_SCHEMA", fields[2]);
    Assertions.assertEquals("metalake.catalog", fields[3]);
    Assertions.assertEquals("{count=7}", fields[7]);
  }

  @Test
  public void testListEventZeroCountInOutput() {
    // count=0 is a valid result (empty list) and must appear in the log.
    ListTableEvent event = new ListTableEvent("eve", Namespace.of("m", "c", "s"), 0);
    String[] fields = new SimpleAuditLogV2(event).toString().split("\t", -1);
    Assertions.assertEquals("{count=0}", fields[7]);
  }

  @Test
  public void testListEventRemoteAddressDefaultsToUnknown() {
    // No RequestContext set — Event subclasses fall back to "unknown".
    ListTableEvent event = new ListTableEvent("frank", Namespace.of("m", "c", "s"), 4);
    SimpleAuditLogV2 log = new SimpleAuditLogV2(event);

    Assertions.assertEquals("unknown", log.remoteAddress());
    String[] fields = log.toString().split("\t", -1);
    Assertions.assertEquals("unknown", fields[6]);
  }

  @Test
  public void testListEventCountMergedWithExistingCustomInfo() {
    // Verify count is prepended when the event also has user-defined customInfo.
    ListTableEvent base = new ListTableEvent("grace", Namespace.of("m", "c", "s"), 9);
    SimpleAuditLogV2 log =
        new SimpleAuditLogV2(base) {
          @Override
          public java.util.Map<String, String> customInfo() {
            return com.google.common.collect.ImmutableMap.of("env", "prod");
          }
        };

    String[] fields = log.toString().split("\t", -1);
    // count must come first, followed by the user-defined key.
    Assertions.assertTrue(fields[7].startsWith("{count=9"));
    Assertions.assertTrue(fields[7].contains("env=prod"));
  }

  @Test
  public void testNonListEventHasEmptyCountField() {
    // A non-list stub event must not have count in the output.
    SimpleAuditLogV2 log = new SimpleAuditLogV2(new StubEvent());
    String[] fields = log.toString().split("\t", -1);
    Assertions.assertEquals("", fields[7]);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedConstructorOmitsCountFromLog() {
    // The deprecated no-count constructor sets resultCount()=-1; the log must not emit {count=-1}.
    ListTableEvent event = new ListTableEvent("henry", Namespace.of("m", "c", "s"));
    String[] fields = new SimpleAuditLogV2(event).toString().split("\t", -1);
    Assertions.assertEquals("", fields[7]);
  }

  // ---- stubs ----

  static class StubEvent extends Event {
    StubEvent() {
      super("test-user", NameIdentifier.of("metalake", "catalog"));
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

  static class StubEventWithCustomInfo extends StubEvent {
    @Override
    public Map<String, String> customInfo() {
      return ImmutableMap.of("k1", "v1");
    }
  }
}
