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
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.model.Model;

/** An abstract class representing a base model. */
@Evolving
public abstract class BaseModel implements Model {

  protected String name;

  @Nullable protected String comment;

  @Nullable protected Map<String, String> properties;

  protected int latestVersion;

  protected AuditInfo auditInfo;

  /**
   * @return The name of the model.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * @return The comment of the model.
   */
  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  /**
   * @return The properties of the model.
   */
  @Nullable
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * @return The latest version of the model.
   */
  @Override
  public int latestVersion() {
    return latestVersion;
  }

  /**
   * @return The audit information of the model.
   */
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  interface Builder<SELF extends Builder<SELF, T>, T extends BaseModel> {

    SELF withName(String name);

    SELF withComment(@Nullable String comment);

    SELF withProperties(@Nullable Map<String, String> properties);

    SELF withLatestVersion(int latestVersion);

    SELF withAuditInfo(AuditInfo auditInfo);

    T build();
  }

  public abstract static class BaseModelBuilder<SELF extends Builder<SELF, T>, T extends BaseModel>
      implements Builder<SELF, T> {

    protected String name;

    @Nullable protected String comment;

    @Nullable protected Map<String, String> properties;

    protected int latestVersion;

    protected AuditInfo auditInfo;

    /**
     * Sets the name of the model.
     *
     * @param name The name of the model.
     * @return This builder instance.
     */
    @Override
    public SELF withName(String name) {
      this.name = name;
      return self();
    }

    /**
     * Sets the comment of the model.
     *
     * @param comment The comment of the model.
     * @return This builder instance.
     */
    @Override
    public SELF withComment(@Nullable String comment) {
      this.comment = comment;
      return self();
    }

    /**
     * Sets the properties of the model.
     *
     * @param properties The properties of the model.
     * @return This builder instance.
     */
    @Override
    public SELF withProperties(@Nullable Map<String, String> properties) {
      this.properties = properties;
      return self();
    }

    /**
     * Sets the latest version of the model.
     *
     * @param latestVersion The latest version of the model.
     * @return This builder instance.
     */
    @Override
    public SELF withLatestVersion(int latestVersion) {
      this.latestVersion = latestVersion;
      return self();
    }

    /**
     * Sets the audit information of the model.
     *
     * @param auditInfo The audit information of the model.
     * @return This builder instance.
     */
    @Override
    public SELF withAuditInfo(AuditInfo auditInfo) {
      this.auditInfo = auditInfo;
      return self();
    }

    /**
     * Builds the model object.
     *
     * @return The model object.
     */
    @Override
    public T build() {
      return internalBuild();
    }

    /**
     * Builds the concrete model object with the provided attributes.
     *
     * @return The concrete model object.
     */
    protected abstract T internalBuild();

    private SELF self() {
      return (SELF) this;
    }
  }
}
