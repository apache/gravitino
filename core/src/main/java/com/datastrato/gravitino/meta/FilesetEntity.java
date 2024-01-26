/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.meta;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.Field;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.file.Fileset;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import lombok.ToString;

@ToString
public class FilesetEntity implements Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the fileset entity.");
  public static final Field NAME =
      Field.required("name", String.class, "The name of the fileset entity.");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment or description of the fileset entity.");
  public static final Field TYPE =
      Field.required("type", Fileset.Type.class, "The type of the fileset entity.");
  public static final Field STORAGE_LOCATION =
      Field.required(
          "storage_location", String.class, "The storage location of the fileset entity.");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the fileset entity.");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the fileset entity.");

  private Long id;

  private String name;

  private Namespace namespace;

  private String comment;

  private Fileset.Type type;

  private String storageLocation;

  private AuditInfo auditInfo;

  private Map<String, String> properties;

  private FilesetEntity() {}

  /**
   * Returns a map of fields and their corresponding values for this fileset entity.
   *
   * @return An unmodifiable map of the fields and values.
   */
  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(TYPE, type);
    fields.put(STORAGE_LOCATION, storageLocation);
    fields.put(AUDIT_INFO, auditInfo);
    fields.put(PROPERTIES, properties);

    return Collections.unmodifiableMap(fields);
  }

  /**
   * Returns the name of the fileset entity.
   *
   * @return The name of the fileset entity.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * Returns the namespace of the fileset entity.
   *
   * @return The namespace of the fileset entity.
   */
  @Override
  public Namespace namespace() {
    return namespace;
  }

  /**
   * Returns the unique id of the fileset entity.
   *
   * @return The unique id of the fileset entity.
   */
  @Override
  public Long id() {
    return id;
  }

  /**
   * Returns the audit details of the fileset entity.
   *
   * @return The audit details of the fileset entity.
   */
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * Returns the type of the entity.
   *
   * @return The type of the entity.
   */
  @Override
  public EntityType type() {
    return EntityType.FILESET;
  }

  /**
   * Returns the comment of the fileset entity.
   *
   * @return The comment of the fileset entity.
   */
  public String comment() {
    return comment;
  }

  /**
   * Returns the type of the fileset entity.
   *
   * @return The type of the fileset entity.
   */
  public Fileset.Type filesetType() {
    return type;
  }

  /**
   * Returns the storage location of the fileset entity.
   *
   * @return The storage location of the fileset entity.
   */
  public String storageLocation() {
    return storageLocation;
  }

  /**
   * Returns the properties of the fileset entity.
   *
   * @return The properties of the fileset entity.
   */
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FilesetEntity)) return false;

    FilesetEntity that = (FilesetEntity) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(comment, that.comment)
        && Objects.equals(type, that.type)
        && Objects.equals(storageLocation, that.storageLocation)
        && Objects.equals(auditInfo, that.auditInfo)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, comment, type, storageLocation, auditInfo, properties);
  }

  public static class Builder {

    private final FilesetEntity fileset;

    public Builder() {
      fileset = new FilesetEntity();
    }

    /**
     * Sets the unique id of the fileset entity.
     *
     * @param id The unique id of the fileset entity.
     * @return The builder instance.
     */
    public Builder withId(Long id) {
      fileset.id = id;
      return this;
    }

    /**
     * Sets the name of the fileset entity.
     *
     * @param name The name of the fileset entity.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      fileset.name = name;
      return this;
    }

    /**
     * Sets the namespace of the fileset entity.
     *
     * @param namespace The namespace of the fileset entity.
     * @return The builder instance.
     */
    public Builder withNamespace(Namespace namespace) {
      fileset.namespace = namespace;
      return this;
    }

    /**
     * Sets the comment of the fileset entity.
     *
     * @param comment The comment of the fileset entity.
     * @return The builder instance.
     */
    public Builder withComment(String comment) {
      fileset.comment = comment;
      return this;
    }

    /**
     * Sets the type of the fileset entity.
     *
     * @param type The type of the fileset entity.
     * @return The builder instance.
     */
    public Builder withFilesetType(Fileset.Type type) {
      fileset.type = type;
      return this;
    }

    /**
     * Sets the storage location of the fileset entity.
     *
     * <p>Only the EXTERNAL type of fileset entity requires a storage location.
     *
     * @param storageLocation The storage location of the fileset entity.
     * @return The builder instance.
     */
    public Builder withStorageLocation(String storageLocation) {
      fileset.storageLocation = storageLocation;
      return this;
    }

    /**
     * Sets the audit details of the fileset entity.
     *
     * @param auditInfo The audit details of the fileset entity.
     * @return The builder instance.
     */
    public Builder withAuditInfo(AuditInfo auditInfo) {
      fileset.auditInfo = auditInfo;
      return this;
    }

    /**
     * Sets the properties of the fileset entity.
     *
     * @param properties The properties of the fileset entity.
     * @return The builder instance.
     */
    public Builder withProperties(Map<String, String> properties) {
      fileset.properties = properties;
      return this;
    }

    /**
     * Builds the fileset entity.
     *
     * @return The fileset entity.
     */
    public FilesetEntity build() {
      fileset.validate();
      return fileset;
    }
  }
}
