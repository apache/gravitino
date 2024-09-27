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

import static org.mockito.Mockito.mock;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.EventListenerManager;
import org.apache.gravitino.listener.api.event.Event;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestAuditManager {
  private static final Logger LOG = LoggerFactory.getLogger(TestAuditManager.class);

  static final Path TEST_AUDIT_LOG_FILE_NAME =
      Paths.get(Configs.AUDIT_LOG_DEFAULT_FILE_WRITER_FILE_NAME);

  @BeforeAll
  public void setup() {
    if (Files.exists(TEST_AUDIT_LOG_FILE_NAME)) {
      LOG.warn(
          String.format(
              "tmp audit log file: %s already exists, delete it",
              Configs.AUDIT_LOG_DEFAULT_FILE_WRITER_FILE_NAME));
      try {
        Files.delete(TEST_AUDIT_LOG_FILE_NAME);
        LOG.warn(
            String.format(
                "delete tmp audit log file: %s success",
                Configs.AUDIT_LOG_DEFAULT_FILE_WRITER_FILE_NAME));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testAuditLog() {
    Config config = mock(Config.class);
    Mockito.doReturn(true).when(config).get(Configs.AUDIT_LOG_ENABLED_CONF);
    Mockito.doReturn("org.apache.gravitino.audit.DummyAuditWriter")
        .when(config)
        .get(Configs.WRITER_CLASS_NAME);

    Mockito.doReturn("org.apache.gravitino.audit.DummyAuditFormatter")
        .when(config)
        .get(Configs.FORMATTER_CLASS_NAME);

    DummyEvent dummyEvent = mockDummyEvent();
    EventListenerManager eventListenerManager = mockEventListenerManager();
    AuditLogManager auditLogManager = mockAuditLogManager(config, eventListenerManager);
    EventBus eventBus = eventListenerManager.createEventBus();
    eventBus.dispatchEvent(dummyEvent);
    Assertions.assertInstanceOf(DummyAuditWriter.class, auditLogManager.getAuditLogWriter());
    Assertions.assertInstanceOf(
        DummyAuditFormatter.class, (auditLogManager.getAuditLogWriter()).getFormatter());

    DummyAuditWriter dummyAuditWriter = (DummyAuditWriter) auditLogManager.getAuditLogWriter();

    DummyAuditFormatter formatter = (DummyAuditFormatter) dummyAuditWriter.getFormatter();
    DummyAuditLog formattedAuditLog = formatter.format(dummyEvent);

    Assertions.assertNotNull(formattedAuditLog);
    Assertions.assertEquals(formattedAuditLog, dummyAuditWriter.getAuditLogs().get(0));
  }

  @Test
  public void testDefaultAuditLog() {
    Config config = mock(Config.class);
    Mockito.doReturn(true).when(config).get(Configs.AUDIT_LOG_ENABLED_CONF);

    DummyEvent dummyEvent = mockDummyEvent();
    EventListenerManager eventListenerManager = mockEventListenerManager();
    AuditLogManager auditLogManager = mockAuditLogManager(config, eventListenerManager);
    EventBus eventBus = eventListenerManager.createEventBus();
    eventBus.dispatchEvent(dummyEvent);
    Assertions.assertInstanceOf(DefaultFileAuditWriter.class, auditLogManager.getAuditLogWriter());
    Assertions.assertInstanceOf(
        DefaultFormatter.class, (auditLogManager.getAuditLogWriter()).getFormatter());

    DefaultFileAuditWriter defaultFileAuditWriter =
        (DefaultFileAuditWriter) auditLogManager.getAuditLogWriter();
    String fileName = defaultFileAuditWriter.fileName;
    String auditLog = readAuditLog(fileName);

    DefaultFormatter formatter = (DefaultFormatter) defaultFileAuditWriter.getFormatter();
    DefaultAuditLog formattedAuditLog = formatter.format(dummyEvent);

    Assertions.assertNotNull(formattedAuditLog);
    Assertions.assertEquals(formattedAuditLog.toString(), auditLog);
  }

  @AfterEach
  public void cleanup() {
    try {
      if (Files.exists(TEST_AUDIT_LOG_FILE_NAME)) {
        Files.delete(TEST_AUDIT_LOG_FILE_NAME);
        LOG.warn(
            String.format(
                "delete tmp audit log file: %s success",
                Configs.AUDIT_LOG_DEFAULT_FILE_WRITER_FILE_NAME));
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

  static class DummyEvent extends Event {
    protected DummyEvent(String user, NameIdentifier identifier) {
      super(user, identifier);
    }
  }
}
