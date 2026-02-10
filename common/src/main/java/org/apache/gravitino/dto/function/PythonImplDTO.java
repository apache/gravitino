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

/** Python implementation DTO. */
@Getter
@EqualsAndHashCode(callSuper = true)
public class PythonImplDTO extends FunctionImplDTO {

  @JsonProperty("handler")
  private String handler;

  @JsonProperty("codeBlock")
  private String codeBlock;

  private PythonImplDTO() {}

  /**
   * Constructor for PythonImplDTO.
   *
   * @param runtime The runtime type.
   * @param resources The function resources.
   * @param properties The properties.
   * @param handler The Python handler function.
   * @param codeBlock The Python code block.
   */
  public PythonImplDTO(
      String runtime,
      FunctionResourcesDTO resources,
      Map<String, String> properties,
      String handler,
      String codeBlock) {
    super(runtime, resources, properties);
    this.handler = handler;
    this.codeBlock = codeBlock;
  }

  @Override
  public FunctionImpl.Language language() {
    return FunctionImpl.Language.PYTHON;
  }

  @Override
  public FunctionImpl toFunctionImpl() {
    return FunctionImpls.ofPython(
        FunctionImpl.RuntimeType.fromString(getRuntime()),
        handler,
        codeBlock,
        getResources() != null ? getResources().toFunctionResources() : null,
        getProperties());
  }
}
