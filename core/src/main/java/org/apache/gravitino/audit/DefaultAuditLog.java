package org.apache.gravitino.audit;

import lombok.Builder;
import lombok.SneakyThrows;
import org.apache.gravitino.json.JsonUtils;

/** The default implementation of the audit log. */
@Builder
public class DefaultAuditLog implements AuditLog {

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

  @SneakyThrows
  public String toString() {
    return JsonUtils.anyFieldMapper().writeValueAsString(this);
  }
}
