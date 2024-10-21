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

import com.google.common.annotations.VisibleForTesting;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DefaultFileAuditWriter is the default implementation of AuditLogWriter, which writes audit logs
 * to a file.
 */
public class FileAuditWriter implements AuditLogWriter {
  private static final Logger Log = LoggerFactory.getLogger(FileAuditWriter.class);

  public static final String AUDIT_LOG_FILE_NAME = "fileName";

  public static final String APPEND = "append";

  public static final String LINE_SEPARATOR = System.lineSeparator();

  Formatter formatter;
  @VisibleForTesting Writer outWriter;
  @VisibleForTesting String fileName;

  boolean append;

  @Override
  public Formatter getFormatter() {
    return formatter;
  }

  @Override
  public void init(Formatter formatter, Map<String, String> properties) {
    this.formatter = formatter;
    this.fileName =
        System.getProperty("gravitino.log.path")
            + "/"
            + properties.getOrDefault(AUDIT_LOG_FILE_NAME, "gravitino_audit.log");
    this.append = Boolean.parseBoolean(properties.getOrDefault(APPEND, "true"));
    try {
      OutputStream outputStream = new FileOutputStream(fileName, append);
      this.outWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new GravitinoRuntimeException(
          e, "Init audit log writer fail, filename is %s", fileName);
    }
  }

  @Override
  public void doWrite(AuditLog auditLog) {
    String log = auditLog.toString();
    try {
      outWriter.write(log + LINE_SEPARATOR);
    } catch (Exception e) {
      Log.warn("Failed to write audit log: {}", log, e);
    }
  }

  @Override
  public void close() {
    if (outWriter != null) {
      try {
        outWriter.close();
      } catch (Exception e) {
        Log.warn("Failed to close writer", e);
      }
    }
  }

  @Override
  public String name() {
    return "file";
  }
}
