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

/** This exception is thrown when an entity is not found. */
public class NoSuchEntityException extends RuntimeException {
  /** The no such entity message for the exception. */
  public static final String NO_SUCH_ENTITY_MESSAGE = "No such %s entity: %s";

  /**
   * Constructs a new NoSuchEntityException.
   *
   * @param message The detail message.
   * @param args The argument of the formatted message.
   */
  @FormatMethod
  public NoSuchEntityException(@FormatString String message, Object... args) {
    super(String.format(message, args));
  }

  /**
   * Constructs a new NoSuchEntityException.
   *
   * @param message The detail message.
   * @param cause The cause of the exception.
   * @param args The argument of the formatted message.
   */
  @FormatMethod
  public NoSuchEntityException(Throwable cause, @FormatString String message, Object... args) {
    super(String.format(message, args), cause);
  }
}
