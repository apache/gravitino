package org.apache.gravitino.audit;

import java.util.Objects;
import org.apache.gravitino.listener.api.event.Event;

public class DefaultFormatter implements Formatter {
  @Override
  public DefaultAuditLog format(Event event) {
    return DefaultAuditLog.builder()
        .user(event.user())
        .eventName(event.getClass().getSimpleName())
        .identifier(
            event.identifier() != null
                ? Objects.requireNonNull(event.identifier()).toString()
                : null)
        .timestamp(String.valueOf(event.eventTime()))
        .build();
  }
}
