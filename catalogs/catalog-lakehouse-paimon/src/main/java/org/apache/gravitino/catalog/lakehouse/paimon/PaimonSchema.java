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
package org.apache.gravitino.catalog.lakehouse.paimon;

import static org.apache.gravitino.meta.AuditInfo.EMPTY;

import java.util.Map;
import java.util.Optional;
import lombok.ToString;
import org.apache.gravitino.Schema;
import org.apache.gravitino.connector.BaseSchema;

/**
 * Implementation of {@link Schema} that represents an Apache Paimon Schema (Database) entity in the
 * Paimon schema.
 */
@ToString
public class PaimonSchema extends BaseSchema {

  private PaimonSchema() {}

  /**
   * Converts {@link PaimonSchema} instance to inner schema.
   *
   * @return The converted inner schema.
   */
  public Map<String, String> toPaimonProperties() {
    return properties;
  }

  /**
   * Creates a new {@link PaimonSchema} instance from inner schema.
   *
   * @param name The name of inner schema.
   * @param properties The properties of inner schema.
   * @return A new {@link PaimonSchema} instance.
   */
  public static PaimonSchema fromPaimonProperties(String name, Map<String, String> properties) {
    return builder()
        .withName(name)
        .withComment(
            Optional.of(properties)
                .map(map -> map.get(PaimonSchemaPropertiesMetadata.COMMENT))
                .orElse(null))
        .withProperties(properties)
        .withAuditInfo(EMPTY)
        .build();
  }

  /** A builder class for constructing {@link PaimonSchema} instance. */
  public static class Builder extends BaseSchemaBuilder<Builder, PaimonSchema> {

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    /**
     * Internal method to build a {@link PaimonSchema} instance using the provided values.
     *
     * @return A new {@link PaimonSchema} instance with the configured values.
     */
    @Override
    protected PaimonSchema internalBuild() {
      PaimonSchema paimonSchema = new PaimonSchema();
      paimonSchema.name = name;

      if (comment != null) {
        paimonSchema.comment = comment;
      } else if (properties != null) {
        paimonSchema.comment = properties.get(PaimonSchemaPropertiesMetadata.COMMENT);
      } else {
        paimonSchema.comment = null;
      }

      paimonSchema.properties = properties;
      paimonSchema.auditInfo = auditInfo;
      return paimonSchema;
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
