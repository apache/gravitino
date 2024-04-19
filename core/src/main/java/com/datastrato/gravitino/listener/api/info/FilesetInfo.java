/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.info;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.file.Fileset;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;

/** Encapsulates read-only information about a fileset, intended for use in event listeners. */
@DeveloperApi
public final class FilesetInfo {
  private final String name;
  @Nullable private final String comment;
  private final Fileset.Type type;
  private final String storageLocation;
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
        fileset.storageLocation(),
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
    this.storageLocation = storageLocation;
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
    return storageLocation;
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
