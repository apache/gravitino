/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.messaging.Topic;
import com.datastrato.gravitino.meta.AuditInfo;
import java.util.Map;
import javax.annotation.Nullable;

public abstract class BaseTopic implements Topic {

  protected String name;
  @Nullable protected String comment;
  @Nullable protected Map<String, String> properties;
  protected Audit auditInfo;

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
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public Audit auditInfo() {
    return auditInfo;
  }

  interface Builder<SELF extends Builder<SELF, T>, T extends BaseTopic> {

    SELF withName(String name);

    SELF withComment(String comment);

    SELF withProperties(Map<String, String> properties);

    SELF withAuditInfo(AuditInfo auditInfo);

    T build();
  }

  public abstract static class BaseTopicBuilder<
          SELF extends BaseTopicBuilder<SELF, T>, T extends BaseTopic>
      implements Builder<SELF, T> {
    protected String name;
    protected String comment;
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
