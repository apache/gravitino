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
package org.apache.gravitino.storage.relational;

import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests the diagnostic fields without depending on a fully formatted log line. */
public class TestEntityChangeLogDiagnostics {
  @Test
  void testAppendLogCarriesChangeIdentityAndTransactionState() {
    LoggerContext context =
        (LoggerContext)
            LogManager.getContext(EntityChangeLogDiagnostics.class.getClassLoader(), false);
    Configuration config = context.getConfiguration();
    List<LogEvent> events = new ArrayList<>();
    AbstractAppender appender =
        new AbstractAppender("entityChangeLogCapture", null, null, true, null) {
          @Override
          public void append(LogEvent event) {
            events.add(event.toImmutable());
          }
        };
    appender.start();
    LoggerConfig logger =
        new LoggerConfig(EntityChangeLogDiagnostics.class.getName(), Level.DEBUG, false);
    logger.addAppender(appender, Level.DEBUG, null);
    config.addLogger(EntityChangeLogDiagnostics.class.getName(), logger);
    context.updateLoggers();
    try {
      EntityChangeLogDiagnostics.logAppended("ml", "TABLE", OperateType.ALTER, "encoded-name");
      Assertions.assertEquals(1, events.size());
      Assertions.assertTrue(
          events.get(0).getMessage().getFormattedMessage().contains("appendedToTransaction"));
      Assertions.assertArrayEquals(
          new Object[] {"ml", "TABLE", OperateType.ALTER, "encoded-name"},
          events.get(0).getMessage().getParameters());
    } finally {
      config.removeLogger(EntityChangeLogDiagnostics.class.getName());
      context.updateLoggers();
      appender.stop();
    }
  }
}
