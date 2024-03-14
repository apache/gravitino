package com.datastrato.gravitino.dto;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.User;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

public class UserDTO implements User {

  @JsonProperty("name")
  private String name;

  @Nullable
  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  /** Default constructor for Jackson deserialization. */
  protected UserDTO() {}

  protected UserDTO(String name, Map<String, String> properties, AuditDTO audit) {
    this.name = name;
    this.properties = properties;
    this.audit = audit;
  }

  /** @return The name of the User DTO. */
  @Override
  public String name() {
    return name;
  }

  /** @return The properties of the User DTO. */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /** @return The audit information of the User DTO. */
  @Override
  public Audit auditInfo() {
    return audit;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for constructing a CatalogDTO instance.
   *
   * @param <S> The type of the builder instance.
   */
  public static class Builder<S extends Builder> {

    /** The name of the catalog. */
    protected String name;

    /** The properties of the catalog. */
    protected Map<String, String> properties;

    /** The audit information of the catalog. */
    protected AuditDTO audit;

    /**
     * Sets the name of the catalog.
     *
     * @param name The name of the catalog.
     * @return The builder instance.
     */
    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    /**
     * Sets the properties of the catalog.
     *
     * @param properties The properties of the catalog.
     * @return The builder instance.
     */
    public S withProperties(Map<String, String> properties) {
      this.properties = properties;
      return (S) this;
    }

    /**
     * Sets the audit information of the catalog.
     *
     * @param audit The audit information of the catalog.
     * @return The builder instance.
     */
    public S withAudit(AuditDTO audit) {
      this.audit = audit;
      return (S) this;
    }

    public UserDTO build() {
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");
      return new UserDTO(name, properties, audit);
    }
  }
}
