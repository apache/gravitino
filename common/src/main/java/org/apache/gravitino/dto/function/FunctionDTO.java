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
package org.apache.gravitino.dto.function;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.Audit;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionSignature;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.types.Type;

/** DTO for {@link Function}. */
@Getter
@EqualsAndHashCode
@ToString
public class FunctionDTO implements Function {

  @JsonProperty("signature")
  private FunctionSignatureDTO signature;

  @JsonProperty("type")
  private FunctionType functionType;

  @JsonProperty("deterministic")
  private boolean deterministic;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("returnType")
  @JsonSerialize(using = JsonUtils.TypeSerializer.class)
  @JsonDeserialize(using = JsonUtils.TypeDeserializer.class)
  private Type returnType;

  @JsonProperty("returnColumns")
  private FunctionColumnDTO[] returnColumns;

  @JsonProperty("impls")
  private FunctionImplDTO[] impls;

  @JsonProperty("version")
  private int version;

  @JsonProperty("audit")
  private AuditDTO audit;

  private FunctionDTO() {}

  private FunctionDTO(
      FunctionSignatureDTO signature,
      FunctionType functionType,
      boolean deterministic,
      String comment,
      Type returnType,
      FunctionColumnDTO[] returnColumns,
      FunctionImplDTO[] impls,
      int version,
      AuditDTO audit) {
    this.signature = signature;
    this.functionType = functionType;
    this.deterministic = deterministic;
    this.comment = comment;
    this.returnType = returnType;
    this.returnColumns = returnColumns;
    this.impls = impls;
    this.version = version;
    this.audit = audit;
  }

  /**
   * Creates a builder for {@link FunctionDTO}.
   *
   * @return Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Converts a {@link Function} to {@link FunctionDTO}.
   *
   * @param function Source function.
   * @return Converted DTO.
   */
  public static FunctionDTO fromFunction(Function function) {
    FunctionImpl[] impls = function.impls();
    FunctionImplDTO[] implDTOs =
        impls == null
            ? new FunctionImplDTO[0]
            : Arrays.stream(impls).map(FunctionImplDTO::from).toArray(FunctionImplDTO[]::new);
    FunctionColumn[] columns = function.returnColumns();
    FunctionColumnDTO[] columnDTOs =
        columns == null
            ? new FunctionColumnDTO[0]
            : Arrays.stream(columns)
                .map(col -> new FunctionColumnDTO(col.name(), col.dataType(), col.comment()))
                .toArray(FunctionColumnDTO[]::new);

    return FunctionDTO.builder()
        .withSignature(
            new FunctionSignatureDTO(
                function.signature().name(),
                Arrays.stream(function.signature().functionParams())
                    .map(
                        param ->
                            new FunctionParamDTO(
                                param.name(),
                                param.dataType(),
                                param.comment(),
                                param.defaultValue()))
                    .toArray(FunctionParamDTO[]::new)))
        .withFunctionType(function.functionType())
        .withDeterministic(function.deterministic())
        .withComment(function.comment())
        .withReturnType(function.returnType())
        .withReturnColumns(columnDTOs)
        .withImpls(implDTOs)
        .withVersion(function.version())
        .withAudit(
            function.auditInfo() == null
                ? null
                : AuditDTO.builder()
                    .withCreator(function.auditInfo().creator())
                    .withCreateTime(function.auditInfo().createTime())
                    .withLastModifier(function.auditInfo().lastModifier())
                    .withLastModifiedTime(function.auditInfo().lastModifiedTime())
                    .build())
        .build();
  }

  /** {@inheritDoc} */
  @Override
  public FunctionSignature signature() {
    return signature == null ? null : signature.toFunctionSignature();
  }

  /** {@inheritDoc} */
  @Override
  public FunctionType functionType() {
    return functionType;
  }

  /** {@inheritDoc} */
  @Override
  public boolean deterministic() {
    return deterministic;
  }

  /** {@inheritDoc} */
  @Override
  public String comment() {
    return comment;
  }

  /** {@inheritDoc} */
  @Override
  public Type returnType() {
    return returnType;
  }

  /** {@inheritDoc} */
  @Override
  public FunctionImpl[] impls() {
    if (impls == null) {
      return new FunctionImpl[0];
    }
    return Arrays.stream(impls).map(FunctionImplDTO::toFunctionImpl).toArray(FunctionImpl[]::new);
  }

  /** {@inheritDoc} */
  @Override
  public FunctionColumn[] returnColumns() {
    if (returnColumns == null) {
      return new FunctionColumn[0];
    }
    return Arrays.stream(returnColumns)
        .map(FunctionColumnDTO::toFunctionColumn)
        .toArray(FunctionColumn[]::new);
  }

  /** {@inheritDoc} */
  @Override
  public Audit auditInfo() {
    return audit;
  }

  /** {@inheritDoc} */
  @Override
  public int version() {
    return version;
  }

  public static class Builder {
    private FunctionSignatureDTO signature;
    private FunctionType functionType;
    private boolean deterministic;
    private String comment;
    private Type returnType;
    private FunctionColumnDTO[] returnColumns;
    private FunctionImplDTO[] impls;
    private int version;
    private AuditDTO audit;

    private Builder() {}

    /**
     * Sets the function signature.
     *
     * @param signature Function signature DTO.
     * @return This builder.
     */
    public Builder withSignature(FunctionSignatureDTO signature) {
      this.signature = signature;
      return this;
    }

    /**
     * Sets function type.
     *
     * @param functionType Function type.
     * @return This builder.
     */
    public Builder withFunctionType(FunctionType functionType) {
      this.functionType = functionType;
      return this;
    }

    /**
     * Sets determinism flag.
     *
     * @param deterministic Whether deterministic.
     * @return This builder.
     */
    public Builder withDeterministic(boolean deterministic) {
      this.deterministic = deterministic;
      return this;
    }

    /**
     * Sets comment.
     *
     * @param comment Function comment.
     * @return This builder.
     */
    public Builder withComment(String comment) {
      this.comment = comment;
      return this;
    }

    /**
     * Sets return type.
     *
     * @param returnType Return type for scalar/aggregate functions.
     * @return This builder.
     */
    public Builder withReturnType(Type returnType) {
      this.returnType = returnType;
      return this;
    }

    /**
     * Sets return columns.
     *
     * @param returnColumns Table function return columns.
     * @return This builder.
     */
    public Builder withReturnColumns(FunctionColumnDTO[] returnColumns) {
      this.returnColumns = returnColumns;
      return this;
    }

    /**
     * Sets implementations.
     *
     * @param impls Function implementations.
     * @return This builder.
     */
    public Builder withImpls(FunctionImplDTO[] impls) {
      this.impls = impls;
      return this;
    }

    /**
     * Sets version.
     *
     * @param version Function version.
     * @return This builder.
     */
    public Builder withVersion(int version) {
      this.version = version;
      return this;
    }

    /**
     * Sets audit info.
     *
     * @param audit Audit DTO.
     * @return This builder.
     */
    public Builder withAudit(AuditDTO audit) {
      this.audit = audit;
      return this;
    }

    /**
     * Builds the DTO.
     *
     * @return Constructed {@link FunctionDTO}.
     */
    public FunctionDTO build() {
      return new FunctionDTO(
          signature,
          functionType,
          deterministic,
          comment,
          returnType,
          returnColumns,
          impls,
          version,
          audit);
    }
  }
}
