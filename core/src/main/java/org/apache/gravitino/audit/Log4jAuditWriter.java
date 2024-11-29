package org.apache.gravitino.audit;

import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Leverage log4j to write audit log. Users should configure separate appender and logger in log4j
 * property file if they wish to write audit log to a separate file.
 */
public class Log4jAuditWriter implements AuditLogWriter {

  private Formatter formatter;
  private Logger auditLogger;

  @Override
  public Formatter getFormatter() {
    return formatter;
  }

  @Override
  public void init(Formatter formatter, Map<String, String> properties) {
    this.formatter = formatter;
    // TODO: make logger name configurable.
    this.auditLogger = LoggerFactory.getLogger("auditLogger");
  }

  @Override
  public void doWrite(AuditLog auditLog) {
    this.auditLogger.info(auditLog.toString());
  }

  @Override
  public String name() {
    return this.getClass().getName();
  }

  @Override
  public void close() throws IOException {}
}
