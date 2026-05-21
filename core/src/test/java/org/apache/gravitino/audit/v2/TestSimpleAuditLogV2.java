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
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.EventSource;
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
