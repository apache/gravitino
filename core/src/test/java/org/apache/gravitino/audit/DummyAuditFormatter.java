package org.apache.gravitino.audit;

import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.FailureEvent;

public class DummyAuditFormatter implements Formatter {
  @Override
  public DummyAuditLog format(Event event) {
    return DummyAuditLog.builder()
        .user(event.user())
        .operation(parseOperation(event))
        .identifier(event.identifier() != null ? event.identifier().toString() : null)
        .timestamp(event.eventTime())
        .successful(!(event instanceof FailureEvent))
        .build();
  }

  private String parseOperation(Event event) {
    final String eventName = event.getClass().getSimpleName();
    if (event instanceof FailureEvent) {
      return StringUtils.removeEnd(eventName, "FailEvent");
    } else {
      return StringUtils.removeEnd(eventName, "Event");
    }
  }
}
