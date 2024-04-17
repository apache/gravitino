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

@DeveloperApi
public final class FilesetInfo {
  private final String name;
  @Nullable private final String comment;
  private final Fileset.Type type;
  private final String storageLocation;
  private final Map<String, String> properties;
  @Nullable private final Audit audit;

  public FilesetInfo(Fileset fileset) {
    this(
        fileset.name(),
        fileset.comment(),
        fileset.type(),
        fileset.storageLocation(),
        fileset.properties(),
        fileset.auditInfo());
  }

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
    if (properties == null) {
      this.properties = ImmutableMap.of();
    } else {
      this.properties = ImmutableMap.<String, String>builder().putAll(properties).build();
    }
    this.audit = audit;
  }

  @Nullable
  public Audit auditInfo() {
    return audit;
  }

  public String name() {
    return name;
  }

  public Fileset.Type getType() {
    return type;
  }

  public String getStorageLocation() {
    return storageLocation;
  }

  @Nullable
  public String comment() {
    return comment;
  }

  public Map<String, String> properties() {
    return properties;
  }
}
