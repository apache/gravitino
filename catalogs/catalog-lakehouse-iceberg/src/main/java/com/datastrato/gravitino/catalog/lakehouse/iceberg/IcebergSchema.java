/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg;

import com.datastrato.gravitino.connector.BaseSchema;
import com.google.common.collect.Maps;
import java.util.Map;
import lombok.ToString;
import org.apache.commons.collections4.MapUtils;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;

/** Represents an Iceberg Schema (Database) entity in the Iceberg schema. */
@ToString
public class IcebergSchema extends BaseSchema {

  private IcebergSchema() {}

  public CreateNamespaceRequest toCreateRequest(Namespace namespace) {
    Map<String, String> meta = Maps.newHashMap();
    if (MapUtils.isNotEmpty(properties)) {
      meta.putAll(properties);
    }
    if (null != comment) {
      meta.put(IcebergSchemaPropertiesMetadata.COMMENT, comment);
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
              ? (null == properties
                  ? null
                  : properties.get(IcebergSchemaPropertiesMetadata.COMMENT))
              : comment;
      icebergSchema.properties = properties;
      icebergSchema.auditInfo = auditInfo;
      return icebergSchema;
    }
  }
}
