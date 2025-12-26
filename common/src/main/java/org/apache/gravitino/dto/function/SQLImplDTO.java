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

/** SQL implementation DTO. */
@Getter
@EqualsAndHashCode(callSuper = true)
public class SQLImplDTO extends FunctionImplDTO {

  @JsonProperty("sql")
  private String sql;

  private SQLImplDTO() {
    super();
  }

  /**
   * Constructor for SQLImplDTO.
   *
   * @param runtime The runtime type.
   * @param resources The function resources.
   * @param properties The properties.
   * @param sql The SQL expression.
   */
  public SQLImplDTO(
      String runtime, FunctionResourcesDTO resources, Map<String, String> properties, String sql) {
    super(runtime, resources, properties);
    this.sql = sql;
  }

  @Override
  public FunctionImpl.Language language() {
    return FunctionImpl.Language.SQL;
  }

  @Override
  public FunctionImpl toFunctionImpl() {
    return FunctionImpls.ofSql(
        FunctionImpl.RuntimeType.fromString(getRuntime()),
        sql,
        getResources() != null ? getResources().toFunctionResources() : null,
        getProperties());
  }
}
