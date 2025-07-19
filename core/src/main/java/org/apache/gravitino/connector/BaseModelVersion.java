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
import org.apache.gravitino.model.ModelVersion;

/** An abstract class representing a base model version. */
@Evolving
public abstract class BaseModelVersion implements ModelVersion {

  protected int version;

  protected String[] aliases;

  @Nullable protected String comment;

  protected Map<String, String> uris;

  protected Map<String, String> properties;

  protected AuditInfo auditInfo;

  /** @return the version of the model object. */
  @Override
  public int version() {
    return version;
  }

  /** @return the aliases of the model version. */
  @Override
  public String[] aliases() {
    return aliases;
  }

  /** @return the comment of the model version. */
  @Override
  public String comment() {
    return comment;
  }

  /** @return the URIs of the model artifact. */
  @Override
  public Map<String, String> uris() {
    return uris;
  }

  /** @return the properties of the model version. */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /** @return the audit details of the model version. */
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  interface Builder<SELF extends Builder<SELF, T>, T extends BaseModelVersion> {

    SELF withVersion(int version);

    SELF withAliases(String[] aliases);

    SELF withComment(String comment);

    SELF withUris(Map<String, String> uris);

    SELF withProperties(Map<String, String> properties);

    SELF withAuditInfo(AuditInfo auditInfo);

    T build();
  }

  public abstract static class BaseModelVersionBuilder<
          SELF extends Builder<SELF, T>, T extends BaseModelVersion>
      implements Builder<SELF, T> {

    protected int version;

    protected String[] aliases;

    protected String comment;

    protected Map<String, String> uris;

    protected Map<String, String> properties;

    protected AuditInfo auditInfo;

    /**
     * Sets the version of the model object.
     *
     * @param version The version of the model object.
     * @return The builder instance.
     */
    @Override
    public SELF withVersion(int version) {
      this.version = version;
      return self();
    }

    /**
     * Sets the aliases of the model version.
     *
     * @param aliases The aliases of the model version.
     * @return The builder instance.
     */
    @Override
    public SELF withAliases(String[] aliases) {
      this.aliases = aliases;
      return self();
    }

    /**
     * Sets the comment of the model version.
     *
     * @param comment The comment of the model version.
     * @return The builder instance.
     */
    @Override
    public SELF withComment(String comment) {
      this.comment = comment;
      return self();
    }

    /**
     * Sets the URIs of the model artifact.
     *
     * @param uris The URIs of the model artifact.
     * @return The builder instance.
     */
    public SELF withUris(Map<String, String> uris) {
      this.uris = uris;
      return self();
    }

    /**
     * Sets the properties of the model version.
     *
     * @param properties The properties of the model version.
     * @return The builder instance.
     */
    @Override
    public SELF withProperties(Map<String, String> properties) {
      this.properties = properties;
      return self();
    }

    /**
     * Sets the audit details of the model version.
     *
     * @param auditInfo The audit details of the model version.
     * @return The builder instance.
     */
    @Override
    public SELF withAuditInfo(AuditInfo auditInfo) {
      this.auditInfo = auditInfo;
      return self();
    }

    /**
     * Builds the model version object.
     *
     * @return The model version object.
     */
    @Override
    public T build() {
      return internalBuild();
    }

    /**
     * Builds the model version object.
     *
     * @return The model version object.
     */
    protected abstract T internalBuild();

    private SELF self() {
      return (SELF) this;
    }
  }
}
