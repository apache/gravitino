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
package org.apache.gravitino.trino.connector.util;

import io.trino.spi.function.SchemaFunctionName;

/**
 * Reads the schema and function name components of a Trino {@link SchemaFunctionName}.
 *
 * <p>Trino 480 turned {@code SchemaFunctionName} into a record exposing {@code schemaName()}/{@code
 * functionName()} accessors.
 */
public final class SchemaFunctionNames {

  private SchemaFunctionNames() {}

  /**
   * Returns the schema part of the given function name.
   *
   * @param name the Trino schema function name to read
   * @return the schema name
   */
  public static String schemaName(SchemaFunctionName name) {
    return name.schemaName();
  }

  /**
   * Returns the function part of the given function name.
   *
   * @param name the Trino schema function name to read
   * @return the function name
   */
  public static String functionName(SchemaFunctionName name) {
    return name.functionName();
  }
}
