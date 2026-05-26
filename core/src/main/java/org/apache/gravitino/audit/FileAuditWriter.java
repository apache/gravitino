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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileAuditWriter is the default implementation of {@link AuditLogWriter}. It delegates all file
 * management (rotation, compression, retention) to Log4j2 via a dedicated logger named {@code
 * gravitino.audit}. Configure the appender in {@code conf/log4j2.properties}.
 */
public class FileAuditWriter implements AuditLogWriter {

  private static final Logger LOG = LoggerFactory.getLogger(FileAuditWriter.class);

  /** Logger name that must match the {@code logger.audit.name} entry in log4j2.properties. */
  static final String AUDIT_LOGGER_NAME = "gravitino.audit";

  private static final Map<String, String> DEPRECATED_KEYS =
      ImmutableMap.of(
          "fileName",
          "configure appender.audit_file.fileName in log4j2.properties",
          "append",
          "configure appender.audit_file.append in log4j2.properties",
          "flushIntervalSecs",
          "configure immediateFlush or an async appender in log4j2.properties");

  private Formatter formatter;
  private Logger auditLogger;

  @Override
  public Formatter getFormatter() {
    return formatter;
  }

  @Override
  public void init(Formatter formatter, Map<String, String> properties) {
    this.formatter = formatter;
    DEPRECATED_KEYS.forEach(
        (key, hint) -> {
          if (properties.containsKey(key)) {
            LOG.warn(
                "FileAuditWriter config key '{}' is deprecated and has no effect. {}", key, hint);
          }
        });
    this.auditLogger = LoggerFactory.getLogger(AUDIT_LOGGER_NAME);
  }

  @Override
  public void doWrite(AuditLog auditLog) {
    auditLogger.info(auditLog.toString());
  }

  @Override
  public void close() {
    // Log4j2 manages the appender lifecycle; nothing to close here.
  }

  @Override
  public String name() {
    return "file";
  }
}
