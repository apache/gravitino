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

import static org.apache.gravitino.audit.AuditLog.Operation;
import static org.apache.gravitino.audit.AuditLog.Status;

import java.util.HashMap;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.audit.v2.SimpleFormatterV2;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.EventListenerManager;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.FailureEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestAuditManager {

  private static final int EVENT_NUM = 2000;

  /** Test audit log with custom audit writer and formatter. */
  @Test
  public void testAuditLog() {
    Config config = new Config(false) {};
    config.set(Configs.AUDIT_LOG_ENABLED_CONF, true);
    config.set(Configs.AUDIT_LOG_WRITER_CLASS_NAME, "org.apache.gravitino.audit.DummyAuditWriter");
    config.set(
        Configs.AUDIT_LOG_FORMATTER_CLASS_NAME, "org.apache.gravitino.audit.DummyAuditFormatter");

    EventListenerManager eventListenerManager = mockEventListenerManager();
    AuditLogManager auditLogManager = mockAuditLogManager(config, eventListenerManager);
    EventBus eventBus = eventListenerManager.createEventBus();

    // dispatch success event
    DummyEvent dummyEvent = mockDummyEvent();
    eventBus.dispatchEvent(dummyEvent);

    Assertions.assertInstanceOf(DummyAuditWriter.class, auditLogManager.getAuditLogWriter());
    Assertions.assertInstanceOf(
        DummyAuditFormatter.class, (auditLogManager.getAuditLogWriter()).getFormatter());

    DummyAuditWriter dummyAuditWriter = (DummyAuditWriter) auditLogManager.getAuditLogWriter();

    DummyAuditFormatter formatter = (DummyAuditFormatter) dummyAuditWriter.getFormatter();
    DummyAuditLog formattedAuditLog = formatter.format(dummyEvent);

    Assertions.assertNotNull(formattedAuditLog);
    Assertions.assertEquals(Operation.UNKNOWN_OPERATION, formattedAuditLog.operation());
    Assertions.assertEquals(Status.SUCCESS, formattedAuditLog.status());
    Assertions.assertEquals("user", formattedAuditLog.user());
    Assertions.assertEquals("a.b.c.d", formattedAuditLog.identifier());
    Assertions.assertEquals(formattedAuditLog.timestamp(), dummyEvent.eventTime());
    Assertions.assertEquals(formattedAuditLog, dummyAuditWriter.getAuditLogs().get(0));

    // dispatch fail event
    DummyFailEvent dummyFailEvent = mockDummyFailEvent();
    eventBus.dispatchEvent(dummyFailEvent);
    DummyAuditLog formattedFailAuditLog = formatter.format(dummyFailEvent);
    Assertions.assertEquals(formattedFailAuditLog, dummyAuditWriter.getAuditLogs().get(1));
    Assertions.assertEquals(Operation.UNKNOWN_OPERATION, formattedFailAuditLog.operation());
    Assertions.assertEquals(Status.FAILURE, formattedFailAuditLog.status());
    Assertions.assertEquals(formattedFailAuditLog, dummyAuditWriter.getAuditLogs().get(1));
  }

  /** Verify that FileAuditWriter is used by default and delegates to SimpleFormatterV2. */
  @Test
  public void testFileAuditLog() {
    Config config = new Config(false) {};
    config.set(Configs.AUDIT_LOG_ENABLED_CONF, true);
    EventListenerManager eventListenerManager = mockEventListenerManager();
    AuditLogManager auditLogManager = mockAuditLogManager(config, eventListenerManager);
    EventBus eventBus = eventListenerManager.createEventBus();

    // Dispatching events must not throw even though no file appender is configured in the test.
    eventBus.dispatchEvent(mockDummyEvent());

    Assertions.assertInstanceOf(FileAuditWriter.class, auditLogManager.getAuditLogWriter());
    Assertions.assertInstanceOf(
        SimpleFormatterV2.class, auditLogManager.getAuditLogWriter().getFormatter());
  }

  /** Verify that dispatching many events does not throw or deadlock. */
  @Test
  public void testBatchEvents() {
    Config config = new Config(false) {};
    config.set(Configs.AUDIT_LOG_ENABLED_CONF, true);
    EventListenerManager eventListenerManager = mockEventListenerManager();
    mockAuditLogManager(config, eventListenerManager);
    EventBus eventBus = eventListenerManager.createEventBus();

    for (int i = 0; i < EVENT_NUM; i++) {
      eventBus.dispatchEvent(mockDummyEvent());
    }
    // No assertion needed — we just verify no exception is thrown for a large batch.
  }

  private AuditLogManager mockAuditLogManager(
      Config config, EventListenerManager eventListenerManager) {
    AuditLogManager auditLogManager = new AuditLogManager();
    auditLogManager.init(config, eventListenerManager);
    return auditLogManager;
  }

  private EventListenerManager mockEventListenerManager() {
    EventListenerManager eventListenerManager = new EventListenerManager();
    eventListenerManager.init(new HashMap<>());
    eventListenerManager.start();
    return eventListenerManager;
  }

  private DummyEvent mockDummyEvent() {
    return new DummyEvent("user", NameIdentifier.of("a", "b", "c", "d"));
  }

  private DummyFailEvent mockDummyFailEvent() {
    return new DummyFailEvent("user", NameIdentifier.of("a", "b", "c", "d"));
  }

  static class DummyEvent extends Event {
    protected DummyEvent(String user, NameIdentifier identifier) {
      super(user, identifier);
    }
  }

  static class DummyFailEvent extends FailureEvent {
    protected DummyFailEvent(String user, NameIdentifier identifier) {
      super(user, identifier, new RuntimeException("dummy exception"));
    }
  }
}
