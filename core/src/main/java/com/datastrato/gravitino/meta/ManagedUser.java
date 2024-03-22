/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.meta;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.Field;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.authorization.User;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.ToString;

/** A class representing a user metadata entity in Gravitino. */
@ToString
public class ManagedUser implements User, Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, " The unique id of the user entity.");
  public static final Field NAME =
      Field.required("name", String.class, "The name of the user entity.");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the user entity.");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the user entity.");

  public static final Field COMMENT = Field.optional("comment", String.class, "The comment of the user entity.");

  public static final Field FIRST_NAME = Field.required("first_name", String.class, "The first name of the user entity.");

  public static final Field LAST_NAME = Field.required("last_name", String.class, "The last name of the user entity");

  public static final Field DISPLAY_NAME = Field.required("display_name", String.class, "The display name of the user entity");

  public static final Field EMAIL_ADDRESS = Field.required("email_address", String.class, "The email address of the user entity");

  public static final Field ACTIVE = Field.required("active", Boolean.class, "The status of the user entity is whether active or not.");

  public static final Field GROUPS = Field.optional("groups", List.class, "The groups of the user entity");

  public static final Field ROLES = Field.optional("roles", List.class, "The roles of the user entity");

  public static final Field DEFAULT_ROLE = Field.optional("default_role", String.class, "The default role of the user entity");

  private Long id;
  private String name;
  private Map<String, String> properties;
  private AuditInfo auditInfo;
  private String metalake;
  private String comment;
  private String firstName;
  private String lastName;
  private String displayName;
  private String emailAddress;
  private boolean active;
  private List<String> groups;
  private List<String> roles;
  private String defaultRole;

  private ManagedUser() {}

  /**
   * Returns a map of fields and their corresponding values for this user entity.
   *
   * @return An unmodifiable map of the fields and values.
   */
  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(AUDIT_INFO, auditInfo);
    fields.put(PROPERTIES, properties);
    fields.put(COMMENT, comment);
    fields.put(DEFAULT_ROLE, defaultRole);
    fields.put(ROLES, roles);
    fields.put(GROUPS, groups);
    fields.put(FIRST_NAME, firstName);
    fields.put(LAST_NAME, lastName);
    fields.put(DISPLAY_NAME, displayName);
    fields.put(EMAIL_ADDRESS, emailAddress);
    fields.put(ACTIVE, active);

    return Collections.unmodifiableMap(fields);
  }

  /**
   * Returns the name of the user.
   *
   * @return The name of the user.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * Returns the unique id of the user.
   *
   * @return The unique id of the user.
   */
  @Override
  public Long id() {
    return id;
  }

  /**
   * Returns the type of the entity.
   *
   * @return The type of the entity.
   */
  @Override
  public EntityType type() {
    return EntityType.USER;
  }

  /**
   * Returns the audit details of the user.
   *
   * @return The audit details of the user.
   */
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * Returns the properties of the user entity.
   *
   * @return The properties of the user entity.
   */
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * The first name of the user.
   *
   * @return The first name of the user.
   */
  @Override
  public String firstName() {
    return firstName;
  }

  /**
   * The last name of the user.
   *
   * @return The last name of the user.
   */
  @Override
  public String lastName() {
    return lastName;
  }

  /**
   * The display name of the user.
   *
   * @return The display name of the user.
   */
  @Override
  public String displayName() {
    return displayName;
  }

  /**
   * The email address of the user.
   *
   * @return The email address of the user.
   */
  @Override
  public String emailAddress() {
    return emailAddress;
  }

  /**
   * The comment of the user.
   *
   * @return The comment of the user.
   */
  @Override
  public String comment() {
    return comment;
  }

  /**
   * The default role of the user.
   *
   * @return The default role of the user.
   */
  @Override
  public String defaultRole() {
    return defaultRole;
  }

  /**
   * The status of the user is whether active or not.
   *
   * @return The status of the user is whether active or not
   */
  @Override
  public boolean active() {
    return active;
  }

  /**
   * The roles of the user.
   *
   * @return The roles of the user.
   */
  @Override
  public List<String> roles() {
    return roles;
  }

  /**
   * The groups of the user.
   *
   * @return The groups of the user.
   */
  @Override
  public List<String> groups() {
    return groups;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ManagedUser)) return false;

    ManagedUser that = (ManagedUser) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(auditInfo, that.auditInfo)
        && Objects.equals(properties, that.properties)
        && Objects.equals(firstName, that.firstName)
        && Objects.equals(lastName, that.lastName)
        && Objects.equals(displayName, that.displayName)
        && Objects.equals(emailAddress, that.emailAddress)
        && Objects.equals(comment, that.comment)
        && Objects.equals(active, that.active)
        && Objects.equals(roles, that.roles)
        && Objects.equals(groups, that.groups)
        && Objects.equals(defaultRole, that.defaultRole);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id,
        name,
        properties,
        auditInfo,
        firstName,
        lastName,
        displayName,
        emailAddress,
        comment,
        active,
        roles,
        groups,
        defaultRole);
  }

  @Override
  public NameIdentifier nameIdentifier() {
    return NameIdentifier.of(metalake, name);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final ManagedUser managedUser;

    private Builder() {
      this.managedUser = new ManagedUser();
    }

    /**
     * Sets the unique id of the user entity.
     *
     * @param id The unique id of the user entity.
     * @return The builder instance.
     */
    public Builder withId(Long id) {
      managedUser.id = id;
      return this;
    }

    /**
     * Sets the name of the user entity.
     *
     * @param name The name of the user entity.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      managedUser.name = name;
      return this;
    }

    /**
     * Sets the properties of the user entity.
     *
     * @param properties The properties of the user entity.
     * @return The builder instance.
     */
    public Builder withProperties(Map<String, String> properties) {
      managedUser.properties = properties;
      return this;
    }

    /**
     * Sets the audit details of the user entity.
     *
     * @param auditInfo The audit details of the user entity.
     * @return The builder instance.
     */
    public Builder withAuditInfo(AuditInfo auditInfo) {
      managedUser.auditInfo = auditInfo;
      return this;
    }

    /**
     * Sets the metalake of the user entity.
     *
     * @param metalake The metalake of the user entity.
     * @return The builder instance.
     */
    public Builder withMetalake(String metalake) {
      managedUser.metalake = metalake;
      return this;
    }

    /**
     * Sets the first name of the user entity.
     *
     * @param firstName The first name of the user entity.
     * @return The builder instance.
     */
    public Builder withFirstName(String firstName) {
      managedUser.firstName = firstName;
      return this;
    }

    /**
     * Sets the last name of the user entity.
     *
     * @param lastName The last name of the user entity.
     * @return The builder instance.
     */
    public Builder withLastName(String lastName) {
      managedUser.lastName = lastName;
      return this;
    }

    /**
     * Sets the display name of the user entity.
     *
     * @param displayName The display name of the user entity.
     * @return The builder instance.
     */
    public Builder withDisplayName(String displayName) {
      managedUser.displayName = displayName;
      return this;
    }

    /**
     * Sets the email address of the user entity.
     *
     * @param emailAddress The email address of the user entity.
     * @return The builder instance.
     */
    public Builder withEmailAddress(String emailAddress) {
      managedUser.emailAddress = emailAddress;
      return this;
    }

    /**
     * Sets the comment of the user entity.
     *
     * @param comment The comment of the user entity.
     * @return The builder instance.
     */
    public Builder withComment(String comment) {
      managedUser.comment = comment;
      return this;
    }

    /**
     * Sets the status of the user entity.
     *
     * @param active The status of the user entity.
     * @return The builder instance.
     */
    public Builder withActive(boolean active) {
      managedUser.active = active;
      return this;
    }

    /**
     * Sets the default role of the user entity.
     *
     * @param defaultRole The default role of the user entity.
     * @return The builder instance.
     */
    public Builder withDefaultRole(String defaultRole) {
      managedUser.defaultRole = defaultRole;
      return this;
    }

    /**
     * Sets the roles of the user entity.
     *
     * @param roles The roles of the user entity.
     * @return The builder instance.
     */
    public Builder withRoles(List<String> roles) {
      managedUser.roles = roles;
      return this;
    }

    /**
     * Sets the groups of the user entity.
     *
     * @param groups The groups of the user entity.
     * @return The builder instance.
     */
    public Builder withGroups(List<String> groups) {
      managedUser.groups = groups;
      return this;
    }

    /**
     * Builds the user entity.
     *
     * @return The built user entity.
     */
    public ManagedUser build() {
      managedUser.validate();
      return managedUser;
    }
  }
}
