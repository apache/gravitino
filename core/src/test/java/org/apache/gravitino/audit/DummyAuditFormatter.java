package org.apache.gravitino.audit;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.listener.api.event.Event;

public class DummyAuditFormatter implements Formatter {
  @Override
  public Object format(Event event) {
    Map<String, String> formatted = Maps.newHashMap();
    formatted.put("eventName", event.getClass().getSimpleName());
    formatted.put("identifier", event.user());
    formatted.put("timestamp", String.valueOf(event.eventTime()));
    return formatted;
  }
}
