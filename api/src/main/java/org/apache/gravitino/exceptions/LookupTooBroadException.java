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
 * A term lookup cannot establish a complete result within its candidate-work budget.
 *
 * <p>This is not an empty match or a successful partial result. Messages must not expose scanned or
 * hidden counts or target identities. A caller may explicitly omit binding expansion or narrow the
 * query, while retaining domain, authorization and approved-only filters. Disabling bindings still
 * enforces the overall lookup budget. REST maps this invalid query to HTTP 400.
 */
public class LookupTooBroadException extends IllegalArgumentException {

  /**
   * Constructs a work-limit error with a formatted detail message.
   *
   * @param message The format string.
   * @param args The format arguments.
   */
  @FormatMethod
  public LookupTooBroadException(@FormatString String message, Object... args) {
    super(String.format(message, args));
  }

  /**
   * Constructs a work-limit error with its cause and a formatted detail message.
   *
   * @param cause The underlying cause.
   * @param message The format string.
   * @param args The format arguments.
   */
  @FormatMethod
  public LookupTooBroadException(Throwable cause, @FormatString String message, Object... args) {
    super(String.format(message, args), cause);
  }
}
