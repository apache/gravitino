/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.info;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.rel.Schema;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;

@DeveloperApi
public final class SchemaInfo {
  private final String name;
  @Nullable private final String comment;
  private final Map<String, String> properties;
  @Nullable private final Audit audit;

  public SchemaInfo(Schema schema) {
    this(schema.name(), schema.comment(), schema.properties(), schema.auditInfo());
  }

  public SchemaInfo(String name, String comment, Map<String, String> properties, Audit audit) {
    this.name = name;
    this.comment = comment;
    if (properties == null) {
      this.properties = ImmutableMap.of();
    } else {
      this.properties = ImmutableMap.<String, String>builder().putAll(properties).build();
    }
    this.audit = audit;
  }

  public String name() {
    return name;
  }

  @Nullable
  public String comment() {
    return comment;
  }

  public Map<String, String> properties() {
    return properties;
  }

  @Nullable
  public Audit audit() {
    return audit;
  }
}
