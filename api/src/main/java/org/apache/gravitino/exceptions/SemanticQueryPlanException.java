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
package org.apache.gravitino.exceptions;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

/**
 * Exception thrown when a Semantic Model query is well-formed but cannot be compiled by the
 * configured query planner.
 *
 * <p>This is distinct from {@link IllegalArgumentException}, which indicates a malformed query.
 */
public class SemanticQueryPlanException extends RuntimeException {

  /**
   * Constructs an exception with a formatted detail message.
   *
   * @param message The detail message format.
   * @param args The arguments referenced by the format specifiers in the detail message.
   */
  @FormatMethod
  public SemanticQueryPlanException(@FormatString String message, Object... args) {
    super(String.format(message, args));
  }

  /**
   * Constructs an exception with a cause and a formatted detail message.
   *
   * @param cause The cause of the exception.
   * @param message The detail message format.
   * @param args The arguments referenced by the format specifiers in the detail message.
   */
  @FormatMethod
  public SemanticQueryPlanException(Throwable cause, @FormatString String message, Object... args) {
    super(String.format(message, args), cause);
  }
}
