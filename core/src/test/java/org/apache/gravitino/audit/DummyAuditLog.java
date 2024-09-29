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

  private String timestamp;

  private Boolean successful;

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
  public String timestamp() {
    return timestamp;
  }

  public Boolean successful() {
    return successful;
  }
}
