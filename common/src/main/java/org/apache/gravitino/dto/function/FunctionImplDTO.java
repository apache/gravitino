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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import java.util.Map;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.rest.RESTRequest;

/** DTO hierarchy for {@link FunctionImpl}. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = FunctionImplDTO.SqlImplDTO.class, name = "sql"),
  @JsonSubTypes.Type(value = FunctionImplDTO.JavaImplDTO.class, name = "java"),
  @JsonSubTypes.Type(value = FunctionImplDTO.PythonImplDTO.class, name = "python")
})
public interface FunctionImplDTO extends RESTRequest {

  /**
   * Converts this DTO to a {@link FunctionImpl}.
   *
   * @return The converted function implementation.
   */
  FunctionImpl toFunctionImpl();

  /** DTO for SQL implementation. */
  @Getter
  @EqualsAndHashCode
  @ToString
  class SqlImplDTO implements FunctionImplDTO {

    @JsonProperty("runtime")
    private String runtime;

    @JsonProperty("sql")
    private String sql;

    @JsonProperty("resources")
    private FunctionResourcesDTO resources;

    @JsonProperty("properties")
    private Map<String, String> properties;

    private SqlImplDTO() {
      this(null, null, null, null);
    }

    /**
     * Creates a SQL implementation DTO.
     *
     * @param runtime Target runtime.
     * @param sql SQL text.
     * @param resources Optional resources.
     * @param properties Implementation properties.
     */
    public SqlImplDTO(
        String runtime,
        String sql,
        FunctionResourcesDTO resources,
        Map<String, String> properties) {
      this.runtime = runtime;
      this.sql = sql;
      this.resources = resources;
      this.properties = properties;
    }

    /** {@inheritDoc} */
    @Override
    public FunctionImpl toFunctionImpl() {
      return FunctionImpl.ofSql(
          FunctionImpl.RuntimeType.fromString(runtime),
          sql,
          resources == null ? null : resources.toResources(),
          properties);
    }

    /** {@inheritDoc} */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(StringUtils.isNotBlank(runtime), "\"runtime\" is required");
      Preconditions.checkArgument(StringUtils.isNotBlank(sql), "\"sql\" is required");
      if (resources != null) {
        resources.toResources();
      }
    }
  }

  /** DTO for Java implementation. */
  @Getter
  @EqualsAndHashCode
  @ToString
  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  class JavaImplDTO implements FunctionImplDTO {

    @JsonProperty("runtime")
    private String runtime;

    @JsonProperty("className")
    private String className;

    @JsonProperty("resources")
    private FunctionResourcesDTO resources;

    @JsonProperty("properties")
    private Map<String, String> properties;

    /**
     * Creates a Java implementation DTO.
     *
     * @param runtime Target runtime.
     * @param className Fully qualified class name.
     * @param resources Optional resources.
     * @param properties Implementation properties.
     */
    public JavaImplDTO(
        String runtime,
        String className,
        FunctionResourcesDTO resources,
        Map<String, String> properties) {
      this.runtime = runtime;
      this.className = className;
      this.resources = resources;
      this.properties = properties;
    }

    /** {@inheritDoc} */
    @Override
    public FunctionImpl toFunctionImpl() {
      return FunctionImpl.ofJava(
          FunctionImpl.RuntimeType.fromString(runtime),
          className,
          resources == null ? null : resources.toResources(),
          properties);
    }

    /** {@inheritDoc} */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(StringUtils.isNotBlank(runtime), "\"runtime\" is required");
      Preconditions.checkArgument(StringUtils.isNotBlank(className), "\"className\" is required");
      if (resources != null) {
        resources.toResources();
      }
    }
  }

  /** DTO for Python implementation. */
  @Getter
  @EqualsAndHashCode
  @ToString
  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  class PythonImplDTO implements FunctionImplDTO {

    @JsonProperty("runtime")
    private String runtime;

    @JsonProperty("handler")
    private String handler;

    @JsonProperty("codeBlock")
    private String codeBlock;

    @JsonProperty("resources")
    private FunctionResourcesDTO resources;

    @JsonProperty("properties")
    private Map<String, String> properties;

    /**
     * Creates a Python implementation DTO.
     *
     * @param runtime Target runtime.
     * @param handler Python handler entry.
     * @param codeBlock Inline code block.
     * @param resources Optional resources.
     * @param properties Implementation properties.
     */
    public PythonImplDTO(
        String runtime,
        String handler,
        String codeBlock,
        FunctionResourcesDTO resources,
        Map<String, String> properties) {
      this.runtime = runtime;
      this.handler = handler;
      this.codeBlock = codeBlock;
      this.resources = resources;
      this.properties = properties;
    }

    /** {@inheritDoc} */
    @Override
    public FunctionImpl toFunctionImpl() {
      return FunctionImpl.ofPython(
          FunctionImpl.RuntimeType.fromString(runtime),
          handler,
          codeBlock,
          resources == null ? null : resources.toResources(),
          properties);
    }

    /** {@inheritDoc} */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(StringUtils.isNotBlank(runtime), "\"runtime\" is required");
      Preconditions.checkArgument(StringUtils.isNotBlank(handler), "\"handler\" is required");
      if (resources != null) {
        resources.toResources();
      }
    }
  }

  /**
   * Converts a {@link FunctionImpl} to its DTO representation.
   *
   * @param impl Source implementation.
   * @return DTO representing the implementation.
   */
  static FunctionImplDTO from(FunctionImpl impl) {
    if (impl instanceof org.apache.gravitino.function.SQLImpl) {
      org.apache.gravitino.function.SQLImpl sqlImpl = (org.apache.gravitino.function.SQLImpl) impl;
      return new SqlImplDTO(
          sqlImpl.runtime().name(),
          sqlImpl.sql(),
          new FunctionResourcesDTO(sqlImpl.resources()),
          sqlImpl.properties());
    } else if (impl instanceof org.apache.gravitino.function.JavaImpl) {
      org.apache.gravitino.function.JavaImpl javaImpl =
          (org.apache.gravitino.function.JavaImpl) impl;
      return new JavaImplDTO(
          javaImpl.runtime().name(),
          javaImpl.className(),
          new FunctionResourcesDTO(javaImpl.resources()),
          javaImpl.properties());
    } else if (impl instanceof org.apache.gravitino.function.PythonImpl) {
      org.apache.gravitino.function.PythonImpl pythonImpl =
          (org.apache.gravitino.function.PythonImpl) impl;
      return new PythonImplDTO(
          pythonImpl.runtime().name(),
          pythonImpl.handler(),
          pythonImpl.codeBlock(),
          new FunctionResourcesDTO(pythonImpl.resources()),
          pythonImpl.properties());
    }

    throw new IllegalArgumentException("Unsupported function implementation: " + impl.getClass());
  }
}
