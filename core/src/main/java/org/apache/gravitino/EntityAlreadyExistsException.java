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
package org.apache.gravitino;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;

/**
 * Exception class indicating that an entity already exists. This exception is thrown when an
 * attempt is made to create an entity that already exists within the Apache Gravitino framework.
 */
public class EntityAlreadyExistsException extends GravitinoRuntimeException {

  /**
   * Constructs an EntityAlreadyExistsException.
   *
   * @param message the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public EntityAlreadyExistsException(@FormatString String message, Object... args) {
    super(message, args);
  }

  /**
   * Constructs an EntityAlreadyExistsException.
   *
   * @param cause the cause.
   * @param message the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public EntityAlreadyExistsException(
      Throwable cause, @FormatString String message, Object... args) {
    super(cause, message, args);
  }
}
