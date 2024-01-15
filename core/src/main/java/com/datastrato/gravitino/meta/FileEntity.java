/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.meta;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.Field;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.file.File;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.ToString;

@ToString
public class FileEntity implements Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the file entity.");
  public static final Field NAME =
      Field.required("name", String.class, "The name of the file entity.");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment or description of the file entity.");
  public static final Field TYPE =
      Field.required("type", File.Type.class, "The type of the file entity.");
  public static final Field STORAGE_LOCATION =
      Field.optional("storage_location", String.class, "The storage location of the file entity.");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the file entity.");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the file entity.");

  private Long id;

  private String name;

  private String comment;

  private File.Type type;

  @Nullable private String storageLocation;

  private AuditInfo auditInfo;

  private Map<String, String> properties;

  private FileEntity() {}

  /**
   * Returns a map of fields and their corresponding values for this file entity.
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
   * Returns the name of the file entity.
   *
   * @return The name of the file entity.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * Returns the unique id of the file entity.
   *
   * @return The unique id of the file entity.
   */
  @Override
  public Long id() {
    return id;
  }

  /**
   * Returns the audit details of the file entity.
   *
   * @return The audit details of the file entity.
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
    return EntityType.FILE;
  }

  /**
   * Returns the comment of the file entity.
   *
   * @return The comment of the file entity.
   */
  public String comment() {
    return comment;
  }

  /**
   * Returns the type of the file entity.
   *
   * @return The type of the file entity.
   */
  public File.Type fileType() {
    return type;
  }

  /**
   * Returns the storage location of the file entity.
   *
   * @return The storage location of the file entity.
   */
  public String storageLocation() {
    return storageLocation;
  }

  /**
   * Returns the properties of the file entity.
   *
   * @return The properties of the file entity.
   */
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FileEntity)) return false;

    FileEntity that = (FileEntity) o;
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

    private final FileEntity file;

    public Builder() {
      file = new FileEntity();
    }

    /**
     * Sets the unique id of the file entity.
     *
     * @param id The unique id of the file entity.
     * @return The builder instance.
     */
    public Builder withId(Long id) {
      file.id = id;
      return this;
    }

    /**
     * Sets the name of the file entity.
     *
     * @param name The name of the file entity.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      file.name = name;
      return this;
    }

    /**
     * Sets the comment of the file entity.
     *
     * @param comment The comment of the file entity.
     * @return The builder instance.
     */
    public Builder withComment(String comment) {
      file.comment = comment;
      return this;
    }

    /**
     * Sets the type of the file entity.
     *
     * @param type The type of the file entity.
     * @return The builder instance.
     */
    public Builder withFileType(File.Type type) {
      file.type = type;
      return this;
    }

    /**
     * Sets the storage location of the file entity.
     *
     * <p>Only the EXTERNAL type of file entity requires a storage location.
     *
     * @param storageLocation The storage location of the file entity.
     * @return The builder instance.
     */
    public Builder withStorageLocation(String storageLocation) {
      file.storageLocation = storageLocation;
      return this;
    }

    /**
     * Sets the audit details of the file entity.
     *
     * @param auditInfo The audit details of the file entity.
     * @return The builder instance.
     */
    public Builder withAuditInfo(AuditInfo auditInfo) {
      file.auditInfo = auditInfo;
      return this;
    }

    /**
     * Sets the properties of the file entity.
     *
     * @param properties The properties of the file entity.
     * @return The builder instance.
     */
    public Builder withProperties(Map<String, String> properties) {
      file.properties = properties;
      return this;
    }

    /**
     * Builds the file entity.
     *
     * @return The file entity.
     */
    public FileEntity build() {
      file.validate();

      if (file.type == File.Type.EXTERNAL && file.storageLocation == null) {
        throw new IllegalArgumentException("Storage location is required for EXTERNAL file.");
      }

      return file;
    }
  }
}
