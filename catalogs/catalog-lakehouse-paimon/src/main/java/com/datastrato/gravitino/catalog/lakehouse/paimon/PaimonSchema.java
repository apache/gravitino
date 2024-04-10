/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon;

import static com.datastrato.gravitino.meta.AuditInfo.EMPTY;

import com.datastrato.gravitino.connector.BaseSchema;
import com.datastrato.gravitino.rel.Schema;
import java.util.Map;
import java.util.Optional;
import lombok.ToString;
import org.apache.paimon.utils.Pair;

/**
 * Implementation of {@link Schema} that represents a Paimon Schema (Database) entity in the Paimon
 * schema.
 */
@ToString
public class PaimonSchema extends BaseSchema {

  private PaimonSchema() {}

  /**
   * Converts {@link PaimonSchema} instance to inner schema.
   *
   * @return The converted inner schema.
   */
  public Pair<String, Map<String, String>> toPaimonSchema() {
    return Pair.of(name, properties);
  }

  /**
   * Creates a new {@link PaimonSchema} instance from inner schema.
   *
   * @param name The name of inner schema.
   * @param properties The properties of inner schema.
   * @return A new {@link PaimonSchema} instance.
   */
  public static PaimonSchema fromPaimonSchema(String name, Map<String, String> properties) {
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
      paimonSchema.comment =
          comment == null
              ? (properties == null ? null : properties.get(PaimonSchemaPropertiesMetadata.COMMENT))
              : comment;
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
