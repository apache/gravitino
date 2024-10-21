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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.stream.Stream;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.EventListenerManager;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.FailureEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestAuditManager {
  private static final Logger LOG = LoggerFactory.getLogger(TestAuditManager.class);

  private static final String DEFAULT_FILE_NAME = "gravitino_audit.log";

  private static final int EVENT_NUM = 2000;

  private Path logPath;

  @BeforeAll
  public void setup() {
    String logDir = System.getProperty("gravitino.log.path");
    Path logDirPath = Paths.get(logDir);
    if (!Files.exists(logDirPath)) {
      try {
        Files.createDirectories(logDirPath);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    this.logPath = Paths.get(logDir + "/" + DEFAULT_FILE_NAME);
    if (Files.exists(logPath)) {
      LOG.warn(
          String.format("tmp audit log file: %s already exists, delete it", DEFAULT_FILE_NAME));
      try {
        Files.delete(logPath);
        LOG.warn(String.format("delete tmp audit log file: %s success", DEFAULT_FILE_NAME));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

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
    Assertions.assertEquals(formattedAuditLog.operation(), Operation.UNKNOWN_OPERATION);
    Assertions.assertEquals(formattedAuditLog.status(), Status.SUCCESS);
    Assertions.assertEquals(formattedAuditLog.user(), "user");
    Assertions.assertEquals(formattedAuditLog.identifier(), "a.b.c.d");
    Assertions.assertEquals(formattedAuditLog.timestamp(), dummyEvent.eventTime());
    Assertions.assertEquals(formattedAuditLog, dummyAuditWriter.getAuditLogs().get(0));

    // dispatch fail event
    DummyFailEvent dummyFailEvent = mockDummyFailEvent();
    eventBus.dispatchEvent(dummyFailEvent);
    DummyAuditLog formattedFailAuditLog = formatter.format(dummyFailEvent);
    Assertions.assertEquals(formattedFailAuditLog, dummyAuditWriter.getAuditLogs().get(1));
    Assertions.assertEquals(formattedFailAuditLog.operation(), Operation.UNKNOWN_OPERATION);
    Assertions.assertEquals(formattedFailAuditLog.status(), Status.FAILURE);
    Assertions.assertEquals(formattedFailAuditLog, dummyAuditWriter.getAuditLogs().get(1));
  }

  /** Test audit log with default audit writer and formatter. */
  @Test
  public void testFileAuditLog() {
    Config config = new Config(false) {};
    config.set(Configs.AUDIT_LOG_ENABLED_CONF, true);
    DummyEvent dummyEvent = mockDummyEvent();
    EventListenerManager eventListenerManager = mockEventListenerManager();
    AuditLogManager auditLogManager = mockAuditLogManager(config, eventListenerManager);
    EventBus eventBus = eventListenerManager.createEventBus();
    eventBus.dispatchEvent(dummyEvent);
    Assertions.assertInstanceOf(FileAuditWriter.class, auditLogManager.getAuditLogWriter());
    Assertions.assertInstanceOf(
        SimpleFormatter.class, (auditLogManager.getAuditLogWriter()).getFormatter());

    FileAuditWriter fileAuditWriter = (FileAuditWriter) auditLogManager.getAuditLogWriter();
    String fileName = fileAuditWriter.fileName;
    try {
      fileAuditWriter.outWriter.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    String auditLog = readAuditLog(fileName);
    Formatter formatter = fileAuditWriter.getFormatter();
    SimpleAuditLog formattedAuditLog = (SimpleAuditLog) formatter.format(dummyEvent);

    Assertions.assertNotNull(formattedAuditLog);
    Assertions.assertEquals(formattedAuditLog.toString(), auditLog);
  }

  @Test
  public void testBathEvents() {
    Config config = new Config(false) {};
    config.set(Configs.AUDIT_LOG_ENABLED_CONF, true);
    // set immediate flush to true for testing, so that the audit log will be read immediately
    config.set(
        new ConfigBuilder("gravitino.audit.writer.file.immediateFlush").stringConf(), "true");

    EventListenerManager eventListenerManager = mockEventListenerManager();
    AuditLogManager auditLogManager = mockAuditLogManager(config, eventListenerManager);
    EventBus eventBus = eventListenerManager.createEventBus();

    for (int i = 0; i < EVENT_NUM; i++) {
      DummyEvent dummyEvent = mockDummyEvent();
      eventBus.dispatchEvent(dummyEvent);
    }

    FileAuditWriter fileAuditWriter = (FileAuditWriter) auditLogManager.getAuditLogWriter();
    String fileName = fileAuditWriter.fileName;
    try {
      fileAuditWriter.outWriter.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    long auditSize = getAuditSize(fileName);
    Assertions.assertEquals(EVENT_NUM, auditSize);
  }

  @AfterEach
  public void cleanup() {
    try {
      if (Files.exists(logPath)) {
        Files.delete(logPath);
        LOG.warn(String.format("delete tmp audit log file: %s success", DEFAULT_FILE_NAME));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String readAuditLog(String fileName) {
    try (BufferedReader reader =
        Files.newBufferedReader(Paths.get(fileName), StandardCharsets.UTF_8)) {
      return reader.readLine();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private long getAuditSize(String fileName) {
    try (Stream<String> lines = Files.lines(Paths.get(fileName))) {
      return lines.count();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
