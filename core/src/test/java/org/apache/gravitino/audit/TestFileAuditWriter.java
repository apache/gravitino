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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.audit.v2.SimpleFormatterV2;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.AbstractConfiguration;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestFileAuditWriter {

  /** Minimal in-memory appender for capturing log events in tests. */
  static class CaptureAppender extends AbstractAppender {
    private final List<LogEvent> events = new ArrayList<>();

    CaptureAppender(String name) {
      super(name, null, PatternLayout.createDefaultLayout(), true, null);
    }

    @Override
    public void append(LogEvent event) {
      events.add(event.toImmutable());
    }

    List<LogEvent> getEvents() {
      return events;
    }
  }

  private CaptureAppender auditCapture;
  private CaptureAppender warnCapture;
  private LoggerContext loggerContext;

  @BeforeEach
  public void setup() {
    loggerContext =
        (LoggerContext) LogManager.getContext(FileAuditWriter.class.getClassLoader(), false);
    Configuration config = loggerContext.getConfiguration();

    auditCapture = new CaptureAppender("auditCapture");
    auditCapture.start();
    config.addAppender(auditCapture);

    // Wire a dedicated logger for gravitino.audit so writes are captured in tests.
    LoggerConfig auditLoggerConfig =
        new LoggerConfig(FileAuditWriter.AUDIT_LOGGER_NAME, Level.INFO, false);
    auditLoggerConfig.addAppender(auditCapture, Level.INFO, null);
    config.addLogger(FileAuditWriter.AUDIT_LOGGER_NAME, auditLoggerConfig);

    // Capture WARN logs from FileAuditWriter itself (for deprecation warning tests).
    warnCapture = new CaptureAppender("warnCapture");
    warnCapture.start();
    config.addAppender(warnCapture);
    String writerLoggerName = FileAuditWriter.class.getName();
    LoggerConfig writerLoggerConfig = new LoggerConfig(writerLoggerName, Level.WARN, false);
    writerLoggerConfig.addAppender(warnCapture, Level.WARN, null);
    config.addLogger(writerLoggerName, writerLoggerConfig);

    loggerContext.updateLoggers();
  }

  @AfterEach
  public void teardown() {
    AbstractConfiguration config = (AbstractConfiguration) loggerContext.getConfiguration();
    config.removeLogger(FileAuditWriter.AUDIT_LOGGER_NAME);
    config.removeLogger(FileAuditWriter.class.getName());
    auditCapture.stop();
    warnCapture.stop();
    config.removeAppender(auditCapture.getName());
    config.removeAppender(warnCapture.getName());
    loggerContext.updateLoggers();
  }

  @Test
  public void testDoWritePublishesToAuditLogger() {
    FileAuditWriter writer = new FileAuditWriter();
    writer.init(new SimpleFormatterV2(), new HashMap<>());

    AuditLog log = new DummyAuditLog();
    writer.doWrite(log);

    Assertions.assertEquals(1, auditCapture.getEvents().size());
    Assertions.assertEquals(
        log.toString(), auditCapture.getEvents().get(0).getMessage().getFormattedMessage());
  }

  @Test
  public void testDeprecatedKeysProduceWarnLogs() {
    FileAuditWriter writer = new FileAuditWriter();
    Map<String, String> properties = new HashMap<>();
    properties.put("fileName", "old-path.log");
    properties.put("append", "true");
    properties.put("flushIntervalSecs", "5");
    writer.init(new SimpleFormatterV2(), properties);

    List<LogEvent> warns = warnCapture.getEvents();
    Assertions.assertEquals(3, warns.size(), "Expected one warning per deprecated key");

    List<String> messages = new ArrayList<>();
    warns.forEach(e -> messages.add(e.getMessage().getFormattedMessage()));
    Assertions.assertTrue(messages.stream().anyMatch(m -> m.contains("fileName")));
    Assertions.assertTrue(messages.stream().anyMatch(m -> m.contains("append")));
    Assertions.assertTrue(messages.stream().anyMatch(m -> m.contains("flushIntervalSecs")));
  }

  @Test
  public void testNoWarnForCleanProperties() {
    FileAuditWriter writer = new FileAuditWriter();
    writer.init(new SimpleFormatterV2(), new HashMap<>());
    Assertions.assertTrue(warnCapture.getEvents().isEmpty());
  }

  @Test
  public void testCloseIsNoOp() {
    FileAuditWriter writer = new FileAuditWriter();
    writer.init(new SimpleFormatterV2(), new HashMap<>());
    // Must not throw.
    Assertions.assertDoesNotThrow(writer::close);
  }

  // ---- helpers ----

  static class DummyAuditLog implements AuditLog {
    @Override
    public String user() {
      return "test-user";
    }

    @Override
    @SuppressWarnings("deprecation")
    public AuditLog.Operation operation() {
      return AuditLog.Operation.UNKNOWN_OPERATION;
    }

    @Override
    public String identifier() {
      return "metalake.catalog";
    }

    @Override
    public long timestamp() {
      return System.currentTimeMillis();
    }

    @Override
    @SuppressWarnings("deprecation")
    public AuditLog.Status status() {
      return AuditLog.Status.SUCCESS;
    }

    @Override
    public String toString() {
      return "dummy-audit-log";
    }
  }
}
