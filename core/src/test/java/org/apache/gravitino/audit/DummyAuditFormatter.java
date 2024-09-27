package org.apache.gravitino.audit;

import org.apache.gravitino.listener.api.event.Event;

public class DummyAuditFormatter implements Formatter {
  @Override
  public DummyAuditLog format(Event event) {
    return DummyAuditLog.builder()
        .user(event.user())
        .eventName(event.getClass().getSimpleName())
        .build();
  }
}
