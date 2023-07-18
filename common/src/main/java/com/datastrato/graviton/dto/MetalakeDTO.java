/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/
package com.datastrato.graviton.dto;

import com.datastrato.graviton.Audit;
import com.datastrato.graviton.Metalake;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class MetalakeDTO implements Metalake {

  @JsonProperty("name")
  private String name;

  @Nullable
  @JsonProperty("comment")
  private String comment;

  @Nullable
  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  protected MetalakeDTO() {}

  protected MetalakeDTO(
      String name, String comment, Map<String, String> properties, AuditDTO audit) {
    this.name = name;
    this.comment = comment;
    this.properties = properties;
    this.audit = audit;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public Audit auditInfo() {
    return audit;
  }

  public static class Builder<S extends Builder> {

    protected String name;
    protected String comment;
    protected Map<String, String> properties;
    protected AuditDTO audit;

    public Builder() {}

    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    public S withComment(String comment) {
      this.comment = comment;
      return (S) this;
    }

    public S withProperties(Map<String, String> properties) {
      this.properties = properties;
      return (S) this;
    }

    public S withAudit(AuditDTO audit) {
      this.audit = audit;
      return (S) this;
    }

    public MetalakeDTO build() {
      Preconditions.checkArgument(name != null && !name.isEmpty(), "name cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");
      return new MetalakeDTO(name, comment, properties, audit);
    }
  }
}
