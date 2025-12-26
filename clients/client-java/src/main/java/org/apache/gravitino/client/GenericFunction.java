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
package org.apache.gravitino.client;

import org.apache.gravitino.Audit;
import org.apache.gravitino.dto.function.FunctionDTO;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.rel.types.Type;

/**
 * Represents a generic function in the Gravitino system. This class is a wrapper around
 * FunctionDTO.
 */
class GenericFunction implements Function {

  private final FunctionDTO functionDTO;

  GenericFunction(FunctionDTO functionDTO) {
    this.functionDTO = functionDTO;
  }

  @Override
  public String name() {
    return functionDTO.name();
  }

  @Override
  public FunctionType functionType() {
    return functionDTO.functionType();
  }

  @Override
  public boolean deterministic() {
    return functionDTO.deterministic();
  }

  @Override
  public String comment() {
    return functionDTO.comment();
  }

  @Override
  public Type returnType() {
    return functionDTO.returnType();
  }

  @Override
  public FunctionColumn[] returnColumns() {
    return functionDTO.returnColumns();
  }

  @Override
  public FunctionDefinition[] definitions() {
    return functionDTO.definitions();
  }

  @Override
  public int version() {
    return functionDTO.version();
  }

  @Override
  public Audit auditInfo() {
    return functionDTO.auditInfo();
  }
}
