package org.apache.gravitino.audit;

import java.util.LinkedList;
import java.util.Map;

public class DummyAuditWriter extends AbstractAuditLogWriter {
  private LinkedList<Object> auditLogs = new LinkedList<>();

  public DummyAuditWriter(Formatter formatter) {
    super(formatter);
  }

  @Override
  public void doWrite(Object event) {
    auditLogs.add(event);
  }

  @Override
  public void init(Map<String, String> config) {}

  @Override
  public void close() {}

  public LinkedList<Object> getAuditLogs() {
    return auditLogs;
  }
}
