/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg;

import com.datastrato.gravitino.catalog.rel.BaseSchema;
import com.google.common.collect.Maps;
import java.util.Map;
import lombok.ToString;
import org.apache.commons.collections4.MapUtils;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;

/** Represents an Iceberg Schema (Database) entity in the Iceberg schema. */
@ToString
public class IcebergSchema extends BaseSchema {

  public static final String ICEBERG_COMMENT_FIELD_NAME = "comment";

  private IcebergSchema() {}

  public CreateNamespaceRequest toCreateRequest(Namespace namespace) {
    Map<String, String> meta = Maps.newHashMap();
    if (MapUtils.isNotEmpty(properties)) {
      meta.putAll(properties);
    }
    if (null != comment) {
      meta.put(ICEBERG_COMMENT_FIELD_NAME, comment);
    }
    return CreateNamespaceRequest.builder().setProperties(meta).withNamespace(namespace).build();
  }

  /** A builder class for constructing IcebergSchema instances. */
  public static class Builder extends BaseSchemaBuilder<Builder, IcebergSchema> {

    @Override
    protected IcebergSchema internalBuild() {
      IcebergSchema icebergSchema = new IcebergSchema();
      icebergSchema.name = name;
      icebergSchema.comment =
          null == comment
              ? (null == properties ? null : properties.get(ICEBERG_COMMENT_FIELD_NAME))
              : comment;
      icebergSchema.properties = properties;
      icebergSchema.auditInfo = auditInfo;
      return icebergSchema;
    }
  }
}
