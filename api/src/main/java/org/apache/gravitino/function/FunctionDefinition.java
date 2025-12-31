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

import org.apache.gravitino.annotation.Evolving;

/**
 * A function definition that pairs a specific parameter list with its implementations. A single
 * function can include multiple definitions (overloads), each with distinct parameters and
 * implementations.
 */
@Evolving
public interface FunctionDefinition {

  /**
   * @return The parameters for this definition. Maybe an empty array for a no-arg definition.
   */
  FunctionParam[] parameters();

  /**
   * @return The implementations associated with this definition.
   */
  FunctionImpl[] impls();
}
