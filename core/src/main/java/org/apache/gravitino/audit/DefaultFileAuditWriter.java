package org.apache.gravitino.audit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Configs;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultFileAuditWriter implements AuditLogWriter {
  private static final Logger Log = LoggerFactory.getLogger(DefaultFileAuditWriter.class);

  private Formatter formatter;
  private Writer outWriter;
  @VisibleForTesting String fileName;

  @Override
  public Formatter getFormatter() {
    return formatter;
  }

  @Override
  public void init(Formatter formatter, Map<String, String> properties) {
    this.formatter = formatter;
    fileName = properties.getOrDefault("file", Configs.AUDIT_LOG_DEFAULT_FILE_WRITER_FILE_NAME);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(fileName), "FileAuditWriter: fileName is not set in configuration.");
    try {
      OutputStream outputStream = new FileOutputStream(fileName, true);
      outWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
    } catch (FileNotFoundException e) {
      throw new GravitinoRuntimeException(
          String.format("Audit log file: %s is not exists", fileName));
    }
  }

  @Override
  public void doWrite(AuditLog auditLog) {
    String log = auditLog.toString();
    try {
      outWriter.write(log);
      outWriter.flush();
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
}
