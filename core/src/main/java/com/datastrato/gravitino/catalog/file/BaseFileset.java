/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.file;

import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.meta.AuditInfo;
import java.util.Map;
import javax.annotation.Nullable;

public abstract class BaseFileset implements Fileset {

  protected String name;

  @Nullable protected String comment;

  protected Type type;

  protected String storageLocation;

  @Nullable protected Map<String, String> properties;

  protected AuditInfo auditInfo;

  @Override
  public String name() {
    return name;
  }

  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public String storageLocation() {
    return storageLocation;
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

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

  public abstract static class BaseFilesetBuilder<
          SELF extends Builder<SELF, T>, T extends BaseFileset>
      implements Builder<SELF, T> {
    protected String name;
    protected String comment;
    protected Type type;
    protected String storageLocation;
    protected Map<String, String> properties;
    protected AuditInfo auditInfo;

    @Override
    public SELF withName(String name) {
      this.name = name;
      return self();
    }

    @Override
    public SELF withComment(String comment) {
      this.comment = comment;
      return self();
    }

    @Override
    public SELF withType(Type type) {
      this.type = type;
      return self();
    }

    @Override
    public SELF withStorageLocation(String storageLocation) {
      this.storageLocation = storageLocation;
      return self();
    }

    @Override
    public SELF withProperties(Map<String, String> properties) {
      this.properties = properties;
      return self();
    }

    @Override
    public SELF withAuditInfo(AuditInfo auditInfo) {
      this.auditInfo = auditInfo;
      return self();
    }

    @Override
    public T build() {
      T t = internalBuild();
      return t;
    }

    private SELF self() {
      return (SELF) this;
    }

    protected abstract T internalBuild();
  }
}
