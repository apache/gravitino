package org.apache.gravitino.audit;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Builder
@ToString
@Getter
@EqualsAndHashCode
public class DummyAuditLog implements AuditLog {

  private String user;

  private String operation;

  private String identifier;

  private long timestamp;

  private boolean successful;

  @Override
  public String user() {
    return user;
  }

  @Override
  public String operation() {
    return operation;
  }

  @Override
  public String identifier() {
    return identifier;
  }

  @Override
  public long timestamp() {
    return timestamp;
  }

  @Override
  public boolean successful() {
    return successful;
  }
}
