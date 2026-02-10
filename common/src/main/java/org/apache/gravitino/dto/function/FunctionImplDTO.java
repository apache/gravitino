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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.JavaImpl;
import org.apache.gravitino.function.PythonImpl;
import org.apache.gravitino.function.SQLImpl;

/** DTO for function implementation. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "language")
@JsonSubTypes({
  @JsonSubTypes.Type(value = SQLImplDTO.class, name = "SQL"),
  @JsonSubTypes.Type(value = JavaImplDTO.class, name = "JAVA"),
  @JsonSubTypes.Type(value = PythonImplDTO.class, name = "PYTHON")
})
@Getter
@EqualsAndHashCode
public abstract class FunctionImplDTO {

  @JsonProperty("runtime")
  private String runtime;

  @JsonProperty("resources")
  private FunctionResourcesDTO resources;

  @JsonProperty("properties")
  private Map<String, String> properties;

  /** Default constructor for Jackson. */
  protected FunctionImplDTO() {}

  /**
   * Constructor for FunctionImplDTO.
   *
   * @param runtime The runtime type.
   * @param resources The function resources.
   * @param properties The properties.
   */
  protected FunctionImplDTO(
      String runtime, FunctionResourcesDTO resources, Map<String, String> properties) {
    this.runtime = runtime;
    this.resources = resources;
    this.properties = properties;
  }

  /**
   * Get the language of this implementation.
   *
   * @return The language.
   */
  public abstract FunctionImpl.Language language();

  /**
   * Convert this DTO to a {@link FunctionImpl} instance.
   *
   * @return The function implementation.
   */
  public abstract FunctionImpl toFunctionImpl();

  /**
   * Create a {@link FunctionImplDTO} from a {@link FunctionImpl} instance.
   *
   * @param impl The function implementation.
   * @return The function implementation DTO.
   */
  public static FunctionImplDTO fromFunctionImpl(FunctionImpl impl) {
    if (impl instanceof SQLImpl) {
      SQLImpl sqlImpl = (SQLImpl) impl;
      return new SQLImplDTO(
          sqlImpl.runtime().name(),
          FunctionResourcesDTO.fromFunctionResources(sqlImpl.resources()),
          sqlImpl.properties(),
          sqlImpl.sql());
    } else if (impl instanceof JavaImpl) {
      JavaImpl javaImpl = (JavaImpl) impl;
      return new JavaImplDTO(
          javaImpl.runtime().name(),
          FunctionResourcesDTO.fromFunctionResources(javaImpl.resources()),
          javaImpl.properties(),
          javaImpl.className());
    } else if (impl instanceof PythonImpl) {
      PythonImpl pythonImpl = (PythonImpl) impl;
      return new PythonImplDTO(
          pythonImpl.runtime().name(),
          FunctionResourcesDTO.fromFunctionResources(pythonImpl.resources()),
          pythonImpl.properties(),
          pythonImpl.handler(),
          pythonImpl.codeBlock());
    }
    throw new IllegalArgumentException("Unsupported implementation type: " + impl.getClass());
  }
}
