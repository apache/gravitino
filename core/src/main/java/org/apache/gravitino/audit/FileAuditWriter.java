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
import java.io.BufferedWriter;
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

  public static final String LOG_FILE_NAME_CONFIG = "fileName";

  public static final String APPEND = "append";

  public static final String FILE_IMMEDIATE_FLUSH_CONFIG = "immediateFlush";

  public static final String BUFFERED = "buffered";

  public static final String BUFFER_SIZE = "bufferSize";

  public static final String LINE_SEPARATOR = "lineSeparator";

  private Formatter formatter;
  private Writer outWriter;
  @VisibleForTesting String fileName;

  boolean immediateFlush;

  String lineSeparator;

  boolean append;

  boolean buffered;

  int bufferSize;

  @Override
  public Formatter getFormatter() {
    return formatter;
  }

  @Override
  public void init(Formatter formatter, Map<String, String> properties) {
    this.formatter = formatter;
    fileName = properties.getOrDefault(LOG_FILE_NAME_CONFIG, "gravitino_audit.log");
    immediateFlush =
        Boolean.parseBoolean(properties.getOrDefault(FILE_IMMEDIATE_FLUSH_CONFIG, "false"));
    lineSeparator = properties.getOrDefault(LINE_SEPARATOR, "\n");
    append = Boolean.parseBoolean(properties.getOrDefault(APPEND, "false"));
    buffered = Boolean.parseBoolean(properties.getOrDefault(BUFFERED, "true"));
    bufferSize = Integer.parseInt(properties.getOrDefault(BUFFER_SIZE, "8192"));
    try {
      OutputStream outputStream = new FileOutputStream(fileName, append);
      outWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new GravitinoRuntimeException(
          e, "Init audit log writer fail, filename is %s", fileName);
    }

    if (buffered) {
      outWriter = new BufferedWriter(outWriter, bufferSize);
    }
  }

  @Override
  public void doWrite(AuditLog auditLog) {
    String log = auditLog.toString();
    try {
      outWriter.write(log + lineSeparator);
      if (immediateFlush) {
        outWriter.flush();
      }
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
