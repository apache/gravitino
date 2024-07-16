/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.connector;

import java.util.Map;
import javax.annotation.Nullable;
import lombok.ToString;
import org.apache.gravitino.Schema;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.meta.AuditInfo;

/** An abstract class representing a base schema in a relational database. */
@Evolving
@ToString
public abstract class BaseSchema implements Schema {

  protected String name;

  @Nullable protected String comment;

  @Nullable protected Map<String, String> properties;

  protected AuditInfo auditInfo;

  /** Returns the name of the schema. */
  @Override
  public String name() {
    return name;
  }

  /** Returns the comment or description for the schema. */
  @Override
  public String comment() {
    return comment;
  }

  /** Returns the associated properties of the schema. */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /** Returns the audit details of the schema. */
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * Builder interface for creating instances of {@link BaseSchema}.
   *
   * @param <SELF> The type of the builder.
   * @param <T> The type of the schema being built.
   */
  interface Builder<SELF extends Builder<SELF, T>, T extends BaseSchema> {

    SELF withName(String name);

    SELF withComment(String comment);

    SELF withProperties(Map<String, String> properties);

    SELF withAuditInfo(AuditInfo auditInfo);

    T build();
  }

  /**
   * An abstract class implementing the builder interface for {@link BaseSchema}.
   *
   * @param <SELF> The type of the builder.
   * @param <T> The type of the schema being built.
   */
  public abstract static class BaseSchemaBuilder<
          SELF extends Builder<SELF, T>, T extends BaseSchema>
      implements Builder<SELF, T> {
    protected String name;
    protected String comment;
    protected Map<String, String> properties;
    protected AuditInfo auditInfo;

    /**
     * Sets the name of the schema.
     *
     * @param name The name of the schema.
     * @return The builder instance.
     */
    @Override
    public SELF withName(String name) {
      this.name = name;
      return self();
    }

    /**
     * Sets the comment of the schema.
     *
     * @param comment The comment or description for the schema.
     * @return The builder instance.
     */
    @Override
    public SELF withComment(String comment) {
      this.comment = comment;
      return self();
    }

    /**
     * Sets the associated properties of the schema.
     *
     * @param properties The associated properties of the schema.
     * @return The builder instance.
     */
    @Override
    public SELF withProperties(Map<String, String> properties) {
      this.properties = properties;
      return self();
    }

    /**
     * Sets the audit details of the schema.
     *
     * @param auditInfo The audit details of the schema.
     * @return The builder instance.
     */
    @Override
    public SELF withAuditInfo(AuditInfo auditInfo) {
      this.auditInfo = auditInfo;
      return self();
    }

    /**
     * Builds the instance of the schema with the provided attributes.
     *
     * @return The built schema instance.
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
     * Builds the concrete instance of the schema with the provided attributes.
     *
     * @return The built schema instance.
     */
    @Evolving
    protected abstract T internalBuild();
  }
}
