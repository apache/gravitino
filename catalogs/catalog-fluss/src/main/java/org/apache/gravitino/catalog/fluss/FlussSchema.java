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

package org.apache.gravitino.catalog.fluss;

import static org.apache.gravitino.StringIdentifier.DUMMY_ID;
import static org.apache.gravitino.StringIdentifier.newPropertiesWithId;

import java.util.Map;
import lombok.ToString;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.DatabaseInfo;
import org.apache.gravitino.Schema;
import org.apache.gravitino.connector.BaseSchema;

/** Represents an Apache Fluss Database as a Gravitino {@link Schema}. */
@ToString
public class FlussSchema extends BaseSchema {

  private FlussSchema() {}

  /**
   * Creates a builder for {@link FlussSchema}.
   *
   * @return the schema builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Converts Gravitino schema metadata to a Fluss database descriptor.
   *
   * @param comment the schema comment
   * @param properties the schema properties
   * @return the converted Fluss database descriptor
   */
  public static DatabaseDescriptor toDatabaseDescriptor(
      String comment, Map<String, String> properties) {
    DatabaseDescriptor.Builder builder =
        DatabaseDescriptor.builder()
            .customProperties(FlussMetadataUtils.removeInternalProperties(properties));
    if (comment != null) {
      builder.comment(comment);
    }
    return builder.build();
  }

  /**
   * Converts a Fluss database info object to a Gravitino schema.
   *
   * @param databaseInfo the Fluss database info
   * @return the converted Gravitino schema
   */
  public static FlussSchema fromDatabaseInfo(DatabaseInfo databaseInfo) {
    DatabaseDescriptor descriptor = databaseInfo.getDatabaseDescriptor();
    return FlussSchema.builder()
        .withName(databaseInfo.getDatabaseName())
        .withComment(descriptor.getComment().orElse(null))
        .withProperties(newPropertiesWithId(DUMMY_ID, descriptor.getCustomProperties()))
        .withAuditInfo(
            FlussMetadataUtils.toAuditInfo(
                databaseInfo.getCreatedTime(), databaseInfo.getModifiedTime()))
        .build();
  }

  /** A builder class for constructing {@link FlussSchema} instances. */
  public static class Builder extends BaseSchemaBuilder<Builder, FlussSchema> {

    @Override
    protected FlussSchema internalBuild() {
      FlussSchema schema = new FlussSchema();
      schema.name = name;
      schema.comment = comment;
      schema.properties = properties;
      schema.auditInfo = auditInfo;
      return schema;
    }
  }
}
