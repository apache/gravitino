/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetCatalog;
import com.datastrato.gravitino.meta.AuditInfo;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An abstract class representing a base fileset for {@link FilesetCatalog}. Developers should
 * extend this class to implement a custom fileset for their fileset catalog.
 */
@Evolving
public abstract class BaseFileset implements Fileset {

  protected String name;

  @Nullable protected String comment;

  protected Type type;

  protected String storageLocation;

  @Nullable protected Map<String, String> properties;

  protected AuditInfo auditInfo;

  /** @return The name of the fileset. */
  @Override
  public String name() {
    return name;
  }

  /** @return The comment or description for the fileset. */
  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  /** @return The {@link Type} of the fileset. */
  @Override
  public Type type() {
    return type;
  }

  /** @return The storage location string of the fileset. */
  @Override
  public String storageLocation() {
    return storageLocation;
  }

  /** @return The audit information for the fileset. */
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /** @return The associated properties of the fileset. */
  @Nullable
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  interface Builder<SELF extends Builder<SELF, T>, T extends BaseFileset> {

    SELF withName(String name);

    SELF withComment(String comment);

    SELF withType(Type type);

    SELF withStorageLocation(String storageLocation);

    SELF withProperties(Map<String, String> properties);

    SELF withAuditInfo(AuditInfo auditInfo);

    T build();
  }

  /**
   * An abstract class implementing the builder interface for {@link BaseFileset}. This class should
   * be extended by the concrete fileset builders.
   *
   * @param <SELF> The type of the builder.
   * @param <T> The type of the fileset being built.
   */
  public abstract static class BaseFilesetBuilder<
          SELF extends Builder<SELF, T>, T extends BaseFileset>
      implements Builder<SELF, T> {
    protected String name;
    protected String comment;
    protected Type type;
    protected String storageLocation;
    protected Map<String, String> properties;
    protected AuditInfo auditInfo;

    /**
     * Sets the name of the fileset.
     *
     * @param name The name of the fileset.
     * @return The builder instance.
     */
    @Override
    public SELF withName(String name) {
      this.name = name;
      return self();
    }

    /**
     * Sets the comment of the fileset.
     *
     * @param comment The comment or description for the fileset.
     * @return The builder instance.
     */
    @Override
    public SELF withComment(String comment) {
      this.comment = comment;
      return self();
    }

    /**
     * Sets the type of the fileset.
     *
     * @param type The type of the fileset.
     * @return The builder instance.
     */
    @Override
    public SELF withType(Type type) {
      this.type = type;
      return self();
    }

    /**
     * Sets the storage location of the fileset.
     *
     * @param storageLocation The storage location of the fileset.
     * @return The builder instance.
     */
    @Override
    public SELF withStorageLocation(String storageLocation) {
      this.storageLocation = storageLocation;
      return self();
    }

    /**
     * Sets the associated properties of the fileset.
     *
     * @param properties The associated properties of the fileset.
     * @return The builder instance.
     */
    @Override
    public SELF withProperties(Map<String, String> properties) {
      this.properties = properties;
      return self();
    }

    /**
     * Sets the audit details of the fileset.
     *
     * @param auditInfo The audit details of the fileset.
     * @return The builder instance.
     */
    @Override
    public SELF withAuditInfo(AuditInfo auditInfo) {
      this.auditInfo = auditInfo;
      return self();
    }

    /**
     * Builds the instance of the fileset with the provided attributes.
     *
     * @return The built fileset instance.
     */
    @Override
    public T build() {
      T t = internalBuild();
      return t;
    }

    private SELF self() {
      return (SELF) this;
    }

    /**
     * Builds the concrete instance of the fileset with the provided attributes.
     *
     * @return The built fileset instance.
     */
    @Evolving
    protected abstract T internalBuild();
  }
}
