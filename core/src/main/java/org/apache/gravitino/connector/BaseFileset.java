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

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.meta.AuditInfo;

/**
 * An abstract class representing a base fileset for {@link FilesetCatalog}. Developers should
 * extend this class to implement a custom fileset for their fileset catalog.
 */
@Evolving
public abstract class BaseFileset implements Fileset {

  protected String name;

  @Nullable protected String comment;

  protected Type type;

  protected Map<String, String> storageLocations;

  @Nullable protected Map<String, String> properties;

  protected AuditInfo auditInfo;

  /**
   * @return The name of the fileset.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * @return The comment or description for the fileset.
   */
  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  /**
   * @return The {@link Type} of the fileset.
   */
  @Override
  public Type type() {
    return type;
  }

  /**
   * @return The storage locations of the fileset.
   */
  @Override
  public Map<String, String> storageLocations() {
    return storageLocations;
  }

  /**
   * @return The audit information for the fileset.
   */
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * @return The associated properties of the fileset.
   */
  @Nullable
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  interface Builder<SELF extends Builder<SELF, T>, T extends BaseFileset> {

    SELF withName(String name);

    SELF withComment(String comment);

    SELF withType(Type type);

    SELF withStorageLocations(Map<String, String> storageLocations);

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
    protected Map<String, String> storageLocations = new HashMap<>();
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
     * Sets the storage locations of the fileset.
     *
     * @param storageLocations The storage locations of the fileset.
     * @return The builder instance.
     */
    @Override
    public SELF withStorageLocations(Map<String, String> storageLocations) {
      this.storageLocations.putAll(storageLocations);
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
