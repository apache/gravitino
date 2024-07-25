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

/** An exception thrown when connect to catalog failed. */
public class ConnectionFailedException extends GravitinoRuntimeException {
  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param cause the cause.
   * @param errorMessageTemplate the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public ConnectionFailedException(
      Throwable cause, @FormatString String errorMessageTemplate, Object... args) {
    super(cause, errorMessageTemplate, args);
  }

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param errorMessageTemplate the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public ConnectionFailedException(@FormatString String errorMessageTemplate, Object... args) {
    super(errorMessageTemplate, args);
  }
}
