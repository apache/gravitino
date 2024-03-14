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

  /**
   * Creates a new instance of UserDTO.
   *
   * @param name The name of the User DTO.
   * @param properties The properties of the User DTO.
   * @param audit The audit information of the User DTO.
   */
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
   * Builder class for constructing a UserDTO instance.
   *
   * @param <S> The type of the builder instance.
   */
  public static class Builder<S extends Builder> {

    /** The name of the user. */
    protected String name;

    /** The properties of the user. */
    protected Map<String, String> properties;

    /** The audit information of the user. */
    protected AuditDTO audit;

    /**
     * Sets the name of the user.
     *
     * @param name The name of the user.
     * @return The builder instance.
     */
    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    /**
     * Sets the properties of the user.
     *
     * @param properties The properties of the user.
     * @return The builder instance.
     */
    public S withProperties(Map<String, String> properties) {
      this.properties = properties;
      return (S) this;
    }

    /**
     * Sets the audit information of the user.
     *
     * @param audit The audit information of the user.
     * @return The builder instance.
     */
    public S withAudit(AuditDTO audit) {
      this.audit = audit;
      return (S) this;
    }


    /**
     * Builds an instance of UserDTO using the builder's properties.
     *
     * @return An instance of UserDTO.
     * @throws IllegalArgumentException If the name or audit are not set.
     */
    public UserDTO build() {
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");
      return new UserDTO(name, properties, audit);
    }
  }
}
