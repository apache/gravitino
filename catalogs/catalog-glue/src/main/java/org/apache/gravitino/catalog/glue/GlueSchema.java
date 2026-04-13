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
package org.apache.gravitino.catalog.glue;

import lombok.ToString;
import org.apache.gravitino.connector.BaseSchema;
import org.apache.gravitino.meta.AuditInfo;
import software.amazon.awssdk.services.glue.model.Database;

/** Represents an AWS Glue Database as a Gravitino {@link org.apache.gravitino.Schema}. */
@ToString
public class GlueSchema extends BaseSchema {

  private GlueSchema() {}

  /**
   * Converts an AWS Glue {@link Database} to a {@link GlueSchema}.
   *
   * <p>Field mapping:
   *
   * <ul>
   *   <li>{@code Database.name()} → {@code name}
   *   <li>{@code Database.description()} → {@code comment} (nullable)
   *   <li>{@code Database.parameters()} → {@code properties}
   *   <li>{@code Database.createTime()} → {@code auditInfo.createTime}
   * </ul>
   *
   * @param database the Glue Database returned by the AWS SDK
   * @return a populated {@link GlueSchema}
   */
  public static GlueSchema fromGlueDatabase(Database database) {
    AuditInfo auditInfo = AuditInfo.builder().withCreateTime(database.createTime()).build();

    return GlueSchema.builder()
        .withName(database.name())
        .withComment(database.description())
        .withProperties(database.parameters())
        .withAuditInfo(auditInfo)
        .build();
  }

  /** Builder for {@link GlueSchema}. */
  public static class Builder extends BaseSchemaBuilder<Builder, GlueSchema> {

    private Builder() {}

    @Override
    protected GlueSchema internalBuild() {
      GlueSchema schema = new GlueSchema();
      schema.name = name;
      schema.comment = comment;
      schema.properties = properties;
      schema.auditInfo = auditInfo;
      return schema;
    }
  }

  /**
   * Creates a new {@link Builder}.
   *
   * @return a new builder instance
   */
  public static Builder builder() {
    return new Builder();
  }
}
