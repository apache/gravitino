package org.apache.gravitino.audit;

import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.FailureEvent;

/** The default implementation of the audit log. */
public class DefaultFormatter implements Formatter {

  @Override
  public DefaultAuditLog format(Event event) {
    Boolean successful = !(event instanceof FailureEvent);
    return DefaultAuditLog.builder()
        .user(event.user())
        .operation(parseOperation(event))
        .identifier(
            event.identifier() != null
                ? Objects.requireNonNull(event.identifier()).toString()
                : null)
        .timestamp(event.eventTime())
        .successful(successful)
        .build();
  }

  private String parseOperation(Event event) {
    final String eventName = event.getClass().getSimpleName();
    if (event instanceof FailureEvent) {
      return StringUtils.removeEnd(eventName, "FailureEvent");
    } else {
      return StringUtils.removeEnd(eventName, "Event");
    }
  }
}
