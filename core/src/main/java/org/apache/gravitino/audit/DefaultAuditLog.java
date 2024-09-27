package org.apache.gravitino.audit;

import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.gravitino.json.JsonUtils;

@Getter
@Builder
public class DefaultAuditLog implements AuditLog {

  private String user;

  private String eventName;

  private String identifier;

  private String timestamp;

  @SneakyThrows
  public String toString() {
    return JsonUtils.anyFieldMapper().writeValueAsString(this);
  }
}
