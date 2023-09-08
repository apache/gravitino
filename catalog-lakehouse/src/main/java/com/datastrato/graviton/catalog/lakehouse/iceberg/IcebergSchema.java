/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg;

import com.datastrato.graviton.meta.rel.BaseSchema;
import com.google.common.collect.Maps;
import lombok.ToString;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;

import javax.annotation.Nullable;
import java.util.Map;

/** Represents an Iceberg Schema (Database) entity in the Iceberg Metastore catalog. */
@ToString
public class IcebergSchema extends BaseSchema {
  private IcebergSchema() {}

  public Namespace getIcebergNamespace(){
    validate();
    return Namespace.of(namespace.levels());
  }

  public CreateNamespaceRequest toCreateRequest(){
    Map<String, String> meta = Maps.newHashMap();
    if (null != properties && !properties.isEmpty()) {
      meta.putAll(properties);
    }
    if(null != comment){
      meta.put("comment", comment);
    }
    return CreateNamespaceRequest.builder()
            .setProperties(meta)
            .withNamespace(Namespace.of(this.namespace.levels()))
            .build();
  }

  /** A builder class for constructing IcebergSchema instances. */
  public static class Builder extends BaseSchemaBuilder<Builder, IcebergSchema> {

    @Override
    protected IcebergSchema internalBuild() {
      IcebergSchema icebergSchema = new IcebergSchema();
      icebergSchema.id = id;
      icebergSchema.catalogId = catalogId;
      icebergSchema.name = name;
      icebergSchema.comment = null == comment ? (null == properties ? null : properties.get("comment")) : comment;
      icebergSchema.properties = properties;
      icebergSchema.auditInfo = auditInfo;
      return icebergSchema;
    }
  }
}
