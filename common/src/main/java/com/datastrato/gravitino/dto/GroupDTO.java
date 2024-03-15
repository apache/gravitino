/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.Group;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/** Represents a Group Data Transfer Object (DTO). */
public class GroupDTO implements Group {

  @JsonProperty("name")
  private String name;

  @Nullable
  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  @JsonProperty("users")
  private List<String> users;

  /** Default constructor for Jackson deserialization. */
  protected GroupDTO() {}

  /**
   * Creates a new instance of GroupDTO.
   *
   * @param name The name of the Group DTO.
   * @param users The collection of users which belongs to the group.
   * @param properties The properties of the Group DTO.
   * @param audit The audit information of the Group DTO.
   */
  protected GroupDTO(
      String name, List<String> users, Map<String, String> properties, AuditDTO audit) {
    this.name = name;
    this.properties = properties;
    this.audit = audit;
    this.users = users;
  }

  /** @return The users of the Group DTO. */
  @Override
  public List<String> users() {
    return users;
  }

  /** @return The name of the Group DTO. */
  @Override
  public String name() {
    return name;
  }

  /** @return The properties of the Group DTO. */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /** @return The audit information of the Group DTO. */
  @Override
  public Audit auditInfo() {
    return audit;
  }

  /**
   * Creates a new Builder for constructing a Group DTO.
   *
   * @return A new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }
  /**
   * Builder class for constructing a GroupDTO instance.
   *
   * @param <S> The type of the builder instance.
   */
  public static class Builder<S extends Builder> {
    /** The name of the group. */
    protected String name;

    /** The properties of the group. */
    protected Map<String, String> properties;

    /** The audit information of the group. */
    protected AuditDTO audit;

    /** The users of the group. */
    protected List<String> users;

    /**
     * Sets the name of the group.
     *
     * @param name The name of the group.
     * @return The builder instance.
     */
    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    /**
     * Sets the properties of the group.
     *
     * @param properties The properties of the group.
     * @return The builder instance.
     */
    public S withProperties(Map<String, String> properties) {
      this.properties = properties;
      return (S) this;
    }

    /**
     * Sets the audit information of the group.
     *
     * @param audit The audit information of the group.
     * @return The builder instance.
     */
    public S withAudit(AuditDTO audit) {
      this.audit = audit;
      return (S) this;
    }

    /**
     * Sets the users of the group.
     *
     * @param users The users of the group.
     * @return The builder instance.
     */
    public S withUsers(List<String> users) {
      this.users = users;
      return (S) this;
    }

    /**
     * Builds an instance of GroupDTO using the builder's properties.
     *
     * @return An instance of GroupDTO.
     * @throws IllegalArgumentException If the name or audit are not set.
     */
    public GroupDTO build() {
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");
      Preconditions.checkArgument(
          CollectionUtils.isNotEmpty(users), "users cannot be null or empty");
      return new GroupDTO(name, users, properties, audit);
    }
  }
}
