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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import com.google.common.collect.Maps;
import java.util.Map;
import lombok.ToString;
import org.apache.commons.collections4.MapUtils;
import org.apache.gravitino.connector.BaseSchema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;

/** Represents an Apache Iceberg Schema (Database) entity in the Iceberg schema. */
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
    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    @Override
    protected IcebergSchema internalBuild() {
      IcebergSchema icebergSchema = new IcebergSchema();
      icebergSchema.name = name;
      if (null == comment) {
        if (null == properties) {
          icebergSchema.comment = null;
        } else {
          icebergSchema.comment = properties.get(IcebergSchemaPropertiesMetadata.COMMENT);
        }
      } else {
        icebergSchema.comment = comment;
      }
      icebergSchema.properties = properties;
      icebergSchema.auditInfo = auditInfo;
      return icebergSchema;
    }
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}
