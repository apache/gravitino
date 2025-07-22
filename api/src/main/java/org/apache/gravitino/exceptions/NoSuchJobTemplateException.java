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
 * An exception thrown when the job template is not existed. This exception is typically used to
 * indicate that a requested job template does not exist in the system.
 */
public class NoSuchJobTemplateException extends NotFoundException {

  /**
   * Constructs a new NoSuchJobTemplateException with the specified detail message.
   *
   * @param message the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public NoSuchJobTemplateException(@FormatString String message, Object... args) {
    super(message, args);
  }

  /**
   * Constructs a new NoSuchJobTemplateException with the specified detail message and cause.
   *
   * @param cause the cause of the exception.
   * @param message the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public NoSuchJobTemplateException(Throwable cause, @FormatString String message, Object... args) {
    super(cause, message, args);
  }
}
