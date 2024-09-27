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
  private String eventName;
}
