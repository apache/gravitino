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
package org.apache.gravitino.meta;

import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import lombok.ToString;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.rel.types.Type;

/**
 * A class representing a function entity in the metadata store.
 *
 * <p>This entity stores both the function metadata and its version information together, avoiding
 * the need for separate FunctionEntity and FunctionVersionEntity. When retrieving, if version is
 * set to the special value {@link #LATEST_VERSION}, the store should return the latest version.
 */
@ToString
public class FunctionEntity implements Entity, Auditable, HasIdentifier, Function {

  /** Special version value indicating the latest version should be retrieved. */
  public static final int LATEST_VERSION = -1;

  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the function entity.");
  public static final Field NAME =
      Field.required("name", String.class, "The name of the function entity.");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment or description of the function entity.");
  public static final Field FUNCTION_TYPE =
      Field.required("function_type", FunctionType.class, "The type of the function.");
  public static final Field DETERMINISTIC =
      Field.required("deterministic", Boolean.class, "Whether the function is deterministic.");
  public static final Field RETURN_TYPE =
      Field.optional(
          "return_type", Type.class, "The return type for scalar or aggregate functions.");
  public static final Field RETURN_COLUMNS =
      Field.optional(
          "return_columns",
          FunctionColumn[].class,
          "The output columns for table-valued functions.");
  public static final Field DEFINITIONS =
      Field.required("definitions", FunctionDefinition[].class, "The definitions of the function.");
  public static final Field VERSION =
      Field.required("version", Integer.class, "The version of the function entity.");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the function entity.");

  private Long id;
  private String name;
  private Namespace namespace;
  private String comment;
  private FunctionType functionType;
  private boolean deterministic;
  private Type returnType;
  private FunctionColumn[] returnColumns;
  private FunctionDefinition[] definitions;
  private Integer version;
  private AuditInfo auditInfo;

  private FunctionEntity() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(FUNCTION_TYPE, functionType);
    fields.put(DETERMINISTIC, deterministic);
    fields.put(RETURN_TYPE, returnType);
    fields.put(RETURN_COLUMNS, returnColumns);
    fields.put(DEFINITIONS, definitions);
    fields.put(VERSION, version);
    fields.put(AUDIT_INFO, auditInfo);

    return Collections.unmodifiableMap(fields);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Long id() {
    return id;
  }

  @Override
  public Namespace namespace() {
    return namespace;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public FunctionType functionType() {
    return functionType;
  }

  @Override
  public boolean deterministic() {
    return deterministic;
  }

  @Override
  public Type returnType() {
    return returnType;
  }

  @Override
  public FunctionColumn[] returnColumns() {
    return returnColumns != null ? returnColumns : new FunctionColumn[0];
  }

  @Override
  public FunctionDefinition[] definitions() {
    return definitions;
  }

  @Override
  public int version() {
    return version;
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  @Override
  public EntityType type() {
    return EntityType.FUNCTION;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof FunctionEntity)) {
      return false;
    }

    FunctionEntity that = (FunctionEntity) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(comment, that.comment)
        && functionType == that.functionType
        && deterministic == that.deterministic
        && Objects.equals(returnType, that.returnType)
        && Arrays.equals(returnColumns, that.returnColumns)
        && Arrays.equals(definitions, that.definitions)
        && Objects.equals(version, that.version)
        && Objects.equals(auditInfo, that.auditInfo);
  }

  @Override
  public int hashCode() {
    int result =
        Objects.hash(
            id,
            name,
            namespace,
            comment,
            functionType,
            deterministic,
            returnType,
            version,
            auditInfo);
    result = 31 * result + Arrays.hashCode(returnColumns);
    result = 31 * result + Arrays.hashCode(definitions);
    return result;
  }

  /**
   * Creates a new builder for constructing a FunctionEntity.
   *
   * @return A new builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder class for creating instances of {@link FunctionEntity}. */
  public static class Builder {
    private final FunctionEntity functionEntity;

    private Builder() {
      functionEntity = new FunctionEntity();
    }

    /**
     * Sets the unique id of the function entity.
     *
     * @param id The unique id.
     * @return This builder instance.
     */
    public Builder withId(Long id) {
      functionEntity.id = id;
      return this;
    }

    /**
     * Sets the name of the function entity.
     *
     * @param name The name of the function.
     * @return This builder instance.
     */
    public Builder withName(String name) {
      functionEntity.name = name;
      return this;
    }

    /**
     * Sets the namespace of the function entity.
     *
     * @param namespace The namespace.
     * @return This builder instance.
     */
    public Builder withNamespace(Namespace namespace) {
      functionEntity.namespace = namespace;
      return this;
    }

    /**
     * Sets the comment of the function entity.
     *
     * @param comment The comment or description.
     * @return This builder instance.
     */
    public Builder withComment(String comment) {
      functionEntity.comment = comment;
      return this;
    }

    /**
     * Sets the function type.
     *
     * @param functionType The type of the function (SCALAR, AGGREGATE, or TABLE).
     * @return This builder instance.
     */
    public Builder withFunctionType(FunctionType functionType) {
      functionEntity.functionType = functionType;
      return this;
    }

    /**
     * Sets whether the function is deterministic.
     *
     * @param deterministic True if the function is deterministic, false otherwise.
     * @return This builder instance.
     */
    public Builder withDeterministic(boolean deterministic) {
      functionEntity.deterministic = deterministic;
      return this;
    }

    /**
     * Sets the return type for scalar or aggregate functions.
     *
     * @param returnType The return type.
     * @return This builder instance.
     */
    public Builder withReturnType(Type returnType) {
      functionEntity.returnType = returnType;
      return this;
    }

    /**
     * Sets the return columns for table-valued functions.
     *
     * @param returnColumns The output columns.
     * @return This builder instance.
     */
    public Builder withReturnColumns(FunctionColumn[] returnColumns) {
      functionEntity.returnColumns = returnColumns;
      return this;
    }

    /**
     * Sets the function definitions.
     *
     * @param definitions The definitions (overloads) of the function.
     * @return This builder instance.
     */
    public Builder withDefinitions(FunctionDefinition[] definitions) {
      functionEntity.definitions = definitions;
      return this;
    }

    /**
     * Sets the version of the function entity.
     *
     * @param version The version number.
     * @return This builder instance.
     */
    public Builder withVersion(Integer version) {
      functionEntity.version = version;
      return this;
    }

    /**
     * Sets the audit information.
     *
     * @param auditInfo The audit information.
     * @return This builder instance.
     */
    public Builder withAuditInfo(AuditInfo auditInfo) {
      functionEntity.auditInfo = auditInfo;
      return this;
    }

    /**
     * Builds the FunctionEntity instance.
     *
     * @return The constructed FunctionEntity.
     */
    public FunctionEntity build() {
      functionEntity.validate();
      return functionEntity;
    }
  }
}
