package org.apache.gravitino.audit;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.EventListenerManager;
import org.apache.gravitino.listener.api.event.Event;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestAuditManager {

  @Test
  public void testAuditLog() {
    DummyEvent dummyEvent = mockDummyEvent();
    EventListenerManager eventListenerManager = mockEventListenerManager();
    AuditLogManager auditLogManager = mockAuditLogManager(eventListenerManager);
    EventBus eventBus = eventListenerManager.createEventBus();
    eventBus.dispatchEvent(dummyEvent);
    Assertions.assertInstanceOf(DummyAuditWriter.class, auditLogManager.getAuditLogWriter());
    Assertions.assertInstanceOf(
        DummyAuditFormatter.class,
        ((DummyAuditWriter) auditLogManager.getAuditLogWriter()).getFormatter());
    DummyAuditWriter dummyAuditWriter = (DummyAuditWriter) auditLogManager.getAuditLogWriter();
    Assertions.assertEquals(1, dummyAuditWriter.getAuditLogs().size());
    Assertions.assertInstanceOf(Map.class, dummyAuditWriter.getAuditLogs().get(0));
  }

  private Map<String, String> createManagerConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("enable", "true");
    properties.put("writer.class", "org.apache.gravitino.audit.DummyAuditWriter");
    properties.put("formatter.class", "org.apache.gravitino.audit.DummyAuditFormatter");
    return properties;
  }

  private AuditLogManager mockAuditLogManager(EventListenerManager eventListenerManager) {
    AuditLogManager auditLogManager = new AuditLogManager();
    Map<String, String> config = createManagerConfig();
    auditLogManager.init(config, eventListenerManager);
    return auditLogManager;
  }

  private EventListenerManager mockEventListenerManager() {
    EventListenerManager eventListenerManager = new EventListenerManager();
    eventListenerManager.init(new HashMap<>());
    eventListenerManager.start();
    return eventListenerManager;
  }

  private DummyEvent mockDummyEvent() {
    return new DummyEvent("user", NameIdentifier.of("a", "b", "c", "d"));
  }

  static class DummyEvent extends Event {
    protected DummyEvent(String user, NameIdentifier identifier) {
      super(user, identifier);
    }
  }
}
