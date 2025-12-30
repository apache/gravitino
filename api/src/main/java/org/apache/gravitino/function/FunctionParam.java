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
package org.apache.gravitino.function;

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.types.Type;

/** Represents a function parameter. */
@Evolving
public interface FunctionParam {

  /**
   * @return The name of the parameter.
   */
  String name();

  /**
   * @return The data type of the parameter.
   */
  Type dataType();

  /**
   * @return The optional comment of the parameter, null if not provided.
   */
  @Nullable
  default String comment() {
    return null;
  }

  /**
   * @return The default value of the parameter if provided, otherwise {@link
   *     org.apache.gravitino.rel.Column#DEFAULT_VALUE_NOT_SET}.
   */
  default Expression defaultValue() {
    return DEFAULT_VALUE_NOT_SET;
  }
}
