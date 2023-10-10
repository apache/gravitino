/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import java.util.Objects;

/**
 * The GravitonTableHandle is used to transform table information between Trino and Graviton, as
 * well as to wrap the inner connector table handle for data access.
 */
public final class GravitonTableHandle implements ConnectorTableHandle {

  private final String schemaName;
  private final String tableName;

  private final ConnectorTableHandle internalTableHandle;

  @JsonCreator
  public GravitonTableHandle(
      @JsonProperty("schemaName") String schemaName,
      @JsonProperty("tableName") String tableName,
      @JsonProperty("internalTableHandle") ConnectorTableHandle internalTableHandle) {
    Preconditions.checkArgument(schemaName != null, "schemaName is not null");
    Preconditions.checkArgument(tableName != null, "tableName is not null");
    Preconditions.checkArgument(internalTableHandle != null, "internalTableHandle is not null");

    this.schemaName = schemaName;
    this.tableName = tableName;
    this.internalTableHandle = internalTableHandle;
  }

  @JsonProperty
  public String getSchemaName() {
    return schemaName;
  }

  @JsonProperty
  public String getTableName() {
    return tableName;
  }

  @JsonProperty
  public ConnectorTableHandle getInternalTableHandle() {
    return internalTableHandle;
  }

  public SchemaTableName toSchemaTableName() {
    return new SchemaTableName(schemaName, tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaName, tableName);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    GravitonTableHandle other = (GravitonTableHandle) obj;
    return Objects.equals(this.schemaName, other.schemaName)
        && Objects.equals(this.tableName, other.tableName);
  }

  @Override
  public String toString() {
    return schemaName + ":" + tableName;
  }
}
