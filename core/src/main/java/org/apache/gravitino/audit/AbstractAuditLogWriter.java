package org.apache.gravitino.audit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.gravitino.listener.api.event.Event;

public abstract class AbstractAuditLogWriter implements AuditLogWriter {

  private Formatter formatter;

  public AbstractAuditLogWriter(Formatter formatter) {
    this.formatter = formatter;
  }

  public void write(Event event) {
    Object formatted = this.formatter.format(event);
    doWrite(formatted);
  }

  public abstract void doWrite(Object event);

  public abstract void close();

  @VisibleForTesting
  Formatter getFormatter() {
    return formatter;
  }
}
