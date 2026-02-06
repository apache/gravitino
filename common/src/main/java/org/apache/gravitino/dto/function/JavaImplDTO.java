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
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionImpls;

/** Java implementation DTO. */
@Getter
@EqualsAndHashCode(callSuper = true)
public class JavaImplDTO extends FunctionImplDTO {

  @JsonProperty("className")
  private String className;

  private JavaImplDTO() {}

  /**
   * Constructor for JavaImplDTO.
   *
   * @param runtime The runtime type.
   * @param resources The function resources.
   * @param properties The properties.
   * @param className The fully qualified class name.
   */
  public JavaImplDTO(
      String runtime,
      FunctionResourcesDTO resources,
      Map<String, String> properties,
      String className) {
    super(runtime, resources, properties);
    this.className = className;
  }

  @Override
  public FunctionImpl.Language language() {
    return FunctionImpl.Language.JAVA;
  }

  @Override
  public FunctionImpl toFunctionImpl() {
    return FunctionImpls.ofJava(
        FunctionImpl.RuntimeType.fromString(getRuntime()),
        className,
        getResources() != null ? getResources().toFunctionResources() : null,
        getProperties());
  }
}
