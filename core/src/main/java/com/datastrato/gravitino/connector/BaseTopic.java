/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.messaging.Topic;
import com.datastrato.gravitino.meta.AuditInfo;
import java.util.Map;
import javax.annotation.Nullable;

/** An abstract class representing a base topic in a messaging system. */
@Evolving
public abstract class BaseTopic implements Topic {

  protected String name;

  @Nullable protected String comment;

  @Nullable protected Map<String, String> properties;

  protected Audit auditInfo;

  /** @return The name of the topic. */
  @Override
  public String name() {
    return name;
  }

  /** @return The comment or description for the topic. */
  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  /** @return The associated properties of the topic. */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /** @return The audit information for the topic. */
  @Override
  public Audit auditInfo() {
    return auditInfo;
  }

  /**
   * Builder interface for {@link BaseTopic}.
   *
   * @param <SELF> The type of the builder.
   * @param <T> The type of the topic being built.
   */
  interface Builder<SELF extends Builder<SELF, T>, T extends BaseTopic> {

    SELF withName(String name);

    SELF withComment(String comment);

    SELF withProperties(Map<String, String> properties);

    SELF withAuditInfo(AuditInfo auditInfo);

    T build();
  }

  /**
   * An abstract class implementing the builder interface for {@link BaseTopic}. This class should
   * be extended by the concrete topic builders.
   *
   * @param <SELF> The type of the builder.
   * @param <T> The type of the topic being built.
   */
  public abstract static class BaseTopicBuilder<
          SELF extends BaseTopicBuilder<SELF, T>, T extends BaseTopic>
      implements Builder<SELF, T> {
    protected String name;
    protected String comment;
    protected Map<String, String> properties;
    protected AuditInfo auditInfo;

    /**
     * Sets the name of the topic.
     *
     * @param name The name of the topic.
     * @return The builder instance.
     */
    @Override
    public SELF withName(String name) {
      this.name = name;
      return self();
    }

    /**
     * Sets the comment of the topic.
     *
     * @param comment The comment or description for the topic.
     * @return The builder instance.
     */
    @Override
    public SELF withComment(String comment) {
      this.comment = comment;
      return self();
    }

    /**
     * Sets the associated properties of the topic.
     *
     * @param properties The associated properties of the topic.
     * @return The builder instance.
     */
    @Override
    public SELF withProperties(Map<String, String> properties) {
      this.properties = properties;
      return self();
    }

    /**
     * Sets the audit information for the topic.
     *
     * @param auditInfo The audit information for the topic.
     * @return The builder instance.
     */
    @Override
    public SELF withAuditInfo(AuditInfo auditInfo) {
      this.auditInfo = auditInfo;
      return self();
    }

    /**
     * Builds the topic with the provided attributes.
     *
     * @return The built topic instance.
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
     * Builds the concrete instance of the topic with the provided attributes.
     *
     * @return The concrete instance of the topic.
     */
    @Evolving
    protected abstract T internalBuild();
  }
}
