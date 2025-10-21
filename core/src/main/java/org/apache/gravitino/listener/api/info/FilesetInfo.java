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

package org.apache.gravitino.listener.api.info;

import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.file.Fileset;

/** Encapsulates read-only information about a fileset, intended for use in event listeners. */
@DeveloperApi
public final class FilesetInfo {
  private final String name;
  @Nullable private final String comment;
  private final Fileset.Type type;
  private final Map<String, String> storageLocations;
  private final Map<String, String> properties;
  @Nullable private final Audit audit;

  /**
   * Constructs a FilesetInfo object from a Fileset instance.
   *
   * @param fileset The source Fileset instance.
   */
  public FilesetInfo(Fileset fileset) {
    this(
        fileset.name(),
        fileset.comment(),
        fileset.type(),
        fileset.storageLocations(),
        fileset.properties(),
        fileset.auditInfo());
  }

  /**
   * Constructs a FilesetInfo object with specified details.
   *
   * @param name The name of the fileset.
   * @param comment An optional comment about the fileset. Can be {@code null}.
   * @param type The type of the fileset.
   * @param storageLocation The storage location of the fileset.
   * @param properties A map of properties associated with the fileset. Can be {@code null}.
   * @param audit Optional audit information. Can be {@code null}.
   */
  public FilesetInfo(
      String name,
      String comment,
      Fileset.Type type,
      String storageLocation,
      Map<String, String> properties,
      Audit audit) {
    this.name = name;
    this.comment = comment;
    this.type = type;
    this.storageLocations =
        storageLocation == null
            ? ImmutableMap.of()
            : ImmutableMap.of(LOCATION_NAME_UNKNOWN, storageLocation);
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    this.audit = audit;
  }

  /**
   * Constructs a FilesetInfo object with specified details.
   *
   * @param name The name of the fileset.
   * @param comment An optional comment about the fileset. Can be {@code null}.
   * @param type The type of the fileset.
   * @param storageLocations The storage locations of the fileset.
   * @param properties A map of properties associated with the fileset. Can be {@code null}.
   * @param audit Optional audit information. Can be {@code null}.
   */
  public FilesetInfo(
      String name,
      String comment,
      Fileset.Type type,
      Map<String, String> storageLocations,
      Map<String, String> properties,
      Audit audit) {
    this.name = name;
    this.comment = comment;
    this.type = type;
    this.storageLocations =
        storageLocations == null ? ImmutableMap.of() : ImmutableMap.copyOf(storageLocations);
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    this.audit = audit;
  }

  /**
   * Returns the audit information.
   *
   * @return The audit information, or {@code null} if not available.
   */
  @Nullable
  public Audit auditInfo() {
    return audit;
  }

  /**
   * Returns the name of the fileset.
   *
   * @return The fileset name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the type of the fileset.
   *
   * @return The fileset type.
   */
  public Fileset.Type type() {
    return type;
  }

  /**
   * Returns the storage location of the fileset.
   *
   * @return The storage location.
   */
  public String storageLocation() {
    return storageLocations.get(LOCATION_NAME_UNKNOWN);
  }

  /**
   * @return The storage locations of the fileset.
   */
  public Map<String, String> storageLocations() {
    return storageLocations;
  }

  /**
   * Returns the optional comment about the fileset.
   *
   * @return The comment, or {@code null} if not provided.
   */
  @Nullable
  public String comment() {
    return comment;
  }

  /**
   * Returns the properties associated with the fileset.
   *
   * @return The properties map.
   */
  public Map<String, String> properties() {
    return properties;
  }
}
