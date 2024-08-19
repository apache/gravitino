package org.apache.gravitino.audit;

import java.util.Map;
import org.apache.gravitino.listener.api.event.Event;

/** Interface for writing the audit log. */
public interface AuditLogWriter {

  /**
   * Initialize the writer with the given configuration.
   *
   * @param config
   */
  void init(Map<String, String> config);

  /**
   * Write the audit event to storage.
   *
   * @param auditLog
   */
  void write(Event auditLog);

  /** Close the writer. */
  void close();
}
